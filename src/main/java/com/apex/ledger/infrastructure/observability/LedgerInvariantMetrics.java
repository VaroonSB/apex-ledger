package com.apex.ledger.infrastructure.observability;

import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import com.apex.ledger.infrastructure.persistence.repository.JournalEntryRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exposes the ledger's correctness invariants as metrics.
 *
 * <p>These are the two most important numbers this system produces, and both must be permanently zero:
 *
 * <ul>
 *   <li>{@code apex.ledger.invariant.unbalanced.currencies} — currencies whose journal entries do not
 *       sum to zero. Non-zero means the ledger as a whole no longer balances.
 *   <li>{@code apex.ledger.invariant.drifted.accounts} — accounts whose stored projection disagrees with
 *       the journal. Non-zero means something wrote the balance columns other than the database trigger.
 * </ul>
 *
 * <p>Everything else the system reports — latency, throughput, error rates — describes how well it is
 * running. These describe whether it is <em>right</em>. A ledger can serve every request successfully
 * and still be broken, and no error rate would show it; that is precisely the failure mode worth paying
 * for a metric to detect.
 *
 * <h2>Why this is scheduled rather than computed on scrape</h2>
 *
 * <p>Both checks aggregate the entire journal. Wiring them directly into a {@link Gauge} would run a
 * full-table aggregation on every Prometheus scrape — every fifteen seconds, forever, over an
 * append-only table that only grows. The gauge would eventually become the heaviest query in the system,
 * and the monitoring would be the outage.
 *
 * <p>So the queries run on a slow schedule and publish into an {@link AtomicLong} that the gauge reads
 * for free. The trade-off is staleness bounded by the interval, which is the right trade: these values
 * change only if something is already badly wrong, and detecting that within minutes is ample.
 *
 * <p>Every instance runs this, so the queries are duplicated N times. That is acceptable at this
 * frequency, and the alternative — leader election for a read-only check — is more machinery than the
 * saving is worth. If the journal grows to where even the scheduled scan is expensive, the answer is a
 * dedicated reconciliation job against a replica, not a shorter interval here.
 */
@Component
public class LedgerInvariantMetrics {

    private static final Logger log = LoggerFactory.getLogger(LedgerInvariantMetrics.class);

    /** How many drifted accounts to name in the log before truncating. */
    private static final int MAX_REPORTED_ACCOUNTS = 20;

    private final JournalEntryRepository journalEntries;
    private final AccountRepository accounts;

    /**
     * {@code -1} until the first check completes.
     *
     * <p>Deliberately not {@code 0}. A gauge that reads zero before anything has been verified is
     * indistinguishable from a gauge reporting a healthy ledger, so an alert on {@code > 0} would go
     * green during startup and stay green if the check never ran at all. {@code -1} means "unknown", and
     * an alert can treat that as its own condition.
     */
    private final AtomicLong unbalancedCurrencies = new AtomicLong(-1);
    private final AtomicLong driftedAccounts = new AtomicLong(-1);
    private final AtomicLong lastCheckEpochSeconds = new AtomicLong(0);

    public LedgerInvariantMetrics(JournalEntryRepository journalEntries,
                                  AccountRepository accounts,
                                  MeterRegistry meterRegistry) {
        this.journalEntries = Objects.requireNonNull(journalEntries);
        this.accounts = Objects.requireNonNull(accounts);

        Gauge.builder("apex.ledger.invariant.unbalanced.currencies", unbalancedCurrencies,
                        AtomicLong::doubleValue)
                .description("Currencies whose journal entries do not sum to zero. MUST be 0; "
                        + "-1 means no check has completed yet.")
                .strongReference(true)
                .register(meterRegistry);

        Gauge.builder("apex.ledger.invariant.drifted.accounts", driftedAccounts,
                        AtomicLong::doubleValue)
                .description("Accounts whose balance projection disagrees with the journal. MUST be 0; "
                        + "-1 means no check has completed yet.")
                .strongReference(true)
                .register(meterRegistry);

        // Freshness of the two gauges above. Without it, a silently dead scheduler leaves stale values
        // that look like a healthy ledger — the classic way a correctness alarm stops alarming.
        Gauge.builder("apex.ledger.invariant.last.check.timestamp", lastCheckEpochSeconds,
                        AtomicLong::doubleValue)
                .description("Unix seconds of the last completed invariant check. Alert if it stops "
                        + "advancing: the gauges above are only as trustworthy as this value is fresh.")
                .strongReference(true)
                .register(meterRegistry);
    }

    /**
     * Re-verifies both invariants.
     *
     * <p>{@code readOnly} so it can be routed to a replica, which is where this belongs once the journal
     * is large. Runs on a virtual thread via the scheduler, so a slow scan occupies no platform thread.
     *
     * <p>Exceptions are swallowed after logging: this is a monitor, and a monitor that propagates kills
     * its own schedule. The freshness gauge is what surfaces a check that has stopped succeeding.
     */
    @Scheduled(
            fixedDelayString = "${apex.ledger.invariant-check.interval:5m}",
            initialDelayString = "${apex.ledger.invariant-check.initial-delay:30s}")
    @Transactional(readOnly = true)
    public void verifyInvariants() {
        try {
            List<Object[]> unbalanced = journalEntries.findCurrenciesThatDoNotBalance();
            unbalancedCurrencies.set(unbalanced.size());
            if (!unbalanced.isEmpty()) {
                // The loudest thing this application can say. Debits no longer equal credits, which
                // means the double-entry guarantee has been violated somewhere the database triggers
                // did not cover — a manual UPDATE with triggers disabled, a restore from an
                // inconsistent backup, corruption.
                unbalanced.forEach(row -> log.error(
                        "LEDGER INVARIANT VIOLATED: currency {} does not balance; net {}",
                        row.length > 0 ? row[0] : "?", row.length > 1 ? row[1] : "?"));
            }

            List<UUID> drifted = accounts.findAccountsWithDriftedBalanceProjection();
            driftedAccounts.set(drifted.size());
            if (!drifted.isEmpty()) {
                log.error("LEDGER INVARIANT VIOLATED: {} account(s) have a balance projection that "
                                + "disagrees with the journal. The journal is authoritative; the "
                                + "projection must be rebuilt. Accounts: {}{}",
                        drifted.size(),
                        drifted.stream().limit(MAX_REPORTED_ACCOUNTS).toList(),
                        drifted.size() > MAX_REPORTED_ACCOUNTS ? " (truncated)" : "");
            }

            lastCheckEpochSeconds.set(System.currentTimeMillis() / 1000L);

            if (unbalanced.isEmpty() && drifted.isEmpty()) {
                log.debug("ledger invariants hold: all currencies balance, no drifted projections");
            }
        } catch (RuntimeException e) {
            // Values are left as they were; the freshness gauge stops advancing and that is the signal.
            log.error("could not verify ledger invariants; the invariant gauges are now stale", e);
        }
    }

    /** Test/diagnostic accessor: currencies failing to balance as of the last completed check. */
    public long unbalancedCurrencyCount() {
        return unbalancedCurrencies.get();
    }

    /** Test/diagnostic accessor: drifted accounts as of the last completed check. */
    public long driftedAccountCount() {
        return driftedAccounts.get();
    }
}
