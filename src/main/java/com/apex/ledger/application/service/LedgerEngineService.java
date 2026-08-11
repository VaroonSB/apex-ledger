package com.apex.ledger.application.service;

import com.apex.ledger.application.port.in.PostTransferCommand;
import com.apex.ledger.application.port.in.PostTransferResult;
import com.apex.ledger.application.port.out.AccountBalanceProjection;
import com.apex.ledger.application.port.out.AccountLockManager;
import com.apex.ledger.application.port.out.IdempotencyGuard;
import com.apex.ledger.config.ApexLedgerProperties;
import com.apex.ledger.domain.event.TransactionSettledEvent;
import com.apex.ledger.domain.exception.AccountNotPostableException;
import com.apex.ledger.domain.exception.CurrencyMismatchException;
import com.apex.ledger.domain.exception.IdempotencyConflictException;
import com.apex.ledger.domain.exception.UnbalancedTransactionException;
import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.domain.model.RequestFingerprint;
import com.apex.ledger.infrastructure.persistence.LedgerConstraintTranslator;
import com.apex.ledger.infrastructure.persistence.entity.Account;
import com.apex.ledger.infrastructure.persistence.entity.JournalEntry;
import com.apex.ledger.infrastructure.persistence.entity.OutboxEvent;
import com.apex.ledger.infrastructure.persistence.entity.Transaction;
import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import com.apex.ledger.infrastructure.persistence.repository.JournalEntryRepository;
import com.apex.ledger.infrastructure.persistence.repository.OutboxEventRepository;
import com.apex.ledger.infrastructure.persistence.repository.TransactionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The posting engine: turns a {@link PostTransferCommand} into a committed, balanced, published
 * transaction.
 *
 * <h2>The pipeline, and why it is ordered this way</h2>
 *
 * <pre>
 *   1. validate double-entry            in memory, no I/O — reject nonsense before spending anything
 *   2. fingerprint the request          SHA-256 of the canonical form
 *   3. idempotency fast path            a replay returns the original outcome without locking
 *   4. ACQUIRE DISTRIBUTED LOCK         sorted account set, explicit wait + lease
 *   5. @Transactional persist           transaction + journal entries + outbox event, one commit
 *   6. read committed balances          authoritative, post-commit
 *   7. write-through the balance cache  still holding the lock
 *   8. RELEASE LOCK                     try-with-resources, same thread that acquired
 * </pre>
 *
 * <p><strong>Step 1 before step 4</strong> because an unbalanced command should never consume a lock or
 * a database connection. <strong>Step 4 before step 5</strong>, and this is the important one: opening
 * the transaction first and then waiting on Redis would hold a pooled JDBC connection for the whole
 * wait. Under contention on one hot account, {@code waitTime} × concurrent callers of connection-holding
 * threads exhausts the 32-connection pool and the whole service stops — contention on a single account
 * becomes a total outage. Locks are acquired outside the transaction, always.
 *
 * <p><strong>Step 7 inside the lock</strong> is what makes the cache safe: balance writes for one
 * account are serialised by the same lock that serialised the postings, so a slow writer cannot install
 * an older balance over a newer one. The cache's fence makes that hold even if the lock is lost.
 *
 * <h2>Virtual threads</h2>
 *
 * <p>Every blocking point here — the Redisson lock wait, JDBC, the Redis write — parks the virtual
 * thread rather than pinning its carrier, because none of them is reached from inside a
 * {@code synchronized} block. This class contains no {@code synchronized} and no {@code ThreadLocal};
 * its only per-request state is on the stack.
 *
 * <p>The lock is acquired and released on the same virtual thread by construction: the
 * try-with-resources in {@link #post} never hands the handle to another thread or an async callback.
 * That is a hard requirement — Redisson identifies a holder by {@code Thread#threadId()}, so releasing
 * from elsewhere silently fails to unlock and leaves the accounts blocked until the lease expires.
 *
 * <p>Note what is <em>not</em> claimed: that the lock makes the ledger correct. PostgreSQL does that.
 * See {@link AccountLockManager} for the fencing argument.
 */
@Service
public class LedgerEngineService {

    private static final Logger log = LoggerFactory.getLogger(LedgerEngineService.class);

    private static final String AGGREGATE_TYPE = "Transaction";

    private final AccountLockManager lockManager;
    private final IdempotencyGuard idempotencyGuard;
    private final AccountRepository accounts;
    private final TransactionRepository transactions;
    private final JournalEntryRepository journalEntries;
    private final OutboxEventRepository outbox;
    private final AccountBalanceProjection balanceProjection;
    private final LedgerConstraintTranslator constraintTranslator;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Duration lockWaitTime;
    private final Duration lockLeaseTime;
    private final String journalEntriesTopic;

    private final Timer postingTimer;
    private final Counter postedCounter;
    private final Counter replayedCounter;
    private final Counter rejectedCounter;

    /**
     * Self-reference used to invoke {@link #persistPosting} through the Spring proxy.
     *
     * <p>Necessary, not stylistic. {@code @Transactional} is implemented by a proxy around this bean, so
     * a plain {@code this.persistPosting(...)} would bypass it entirely and each repository save would
     * run in its own auto-commit — the transaction, the journal entries and the outbox row would no
     * longer be atomic, silently. {@code ObjectProvider} resolves lazily, so this does not create a
     * circular bean dependency at startup.
     */
    private final ObjectProvider<LedgerEngineService> self;

    public LedgerEngineService(AccountLockManager lockManager,
                              IdempotencyGuard idempotencyGuard,
                              AccountRepository accounts,
                              TransactionRepository transactions,
                              JournalEntryRepository journalEntries,
                              OutboxEventRepository outbox,
                              AccountBalanceProjection balanceProjection,
                              LedgerConstraintTranslator constraintTranslator,
                              ObjectMapper objectMapper,
                              Clock clock,
                              ApexLedgerProperties properties,
                              MeterRegistry meterRegistry,
                              ObjectProvider<LedgerEngineService> self) {
        this.lockManager = Objects.requireNonNull(lockManager);
        this.idempotencyGuard = Objects.requireNonNull(idempotencyGuard);
        this.accounts = Objects.requireNonNull(accounts);
        this.transactions = Objects.requireNonNull(transactions);
        this.journalEntries = Objects.requireNonNull(journalEntries);
        this.outbox = Objects.requireNonNull(outbox);
        this.balanceProjection = Objects.requireNonNull(balanceProjection);
        this.constraintTranslator = Objects.requireNonNull(constraintTranslator);
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.clock = Objects.requireNonNull(clock);
        this.self = Objects.requireNonNull(self);
        this.lockWaitTime = properties.locking().waitTime();
        this.lockLeaseTime = properties.locking().leaseTime();
        this.journalEntriesTopic = properties.topics().journalEntries();

        this.postingTimer = Timer.builder("apex.ledger.posting")
                .description("End-to-end posting latency, including lock acquisition")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.postedCounter = Counter.builder("apex.ledger.posting.result")
                .tag("outcome", "posted").register(meterRegistry);
        this.replayedCounter = Counter.builder("apex.ledger.posting.result")
                .tag("outcome", "replayed").register(meterRegistry);
        this.rejectedCounter = Counter.builder("apex.ledger.posting.result")
                .tag("outcome", "rejected").register(meterRegistry);
    }

    /**
     * Posts a transaction, or returns the original outcome if this submission is a replay.
     *
     * @throws com.apex.ledger.domain.exception.AccountLockTimeoutException if the accounts are too
     *     contended to acquire within the configured wait time. Nothing was written; retry with backoff.
     * @throws UnbalancedTransactionException if the legs do not sum to zero per currency
     * @throws IdempotencyConflictException if the key was reused for a different request. A replay of
     *     an <em>identical</em> request is not an error and returns a result with
     *     {@code replayed = true}.
     * @throws com.apex.ledger.domain.exception.InsufficientFundsException if a leg would push an account
     *     below its floor
     * @throws CurrencyMismatchException if a leg's currency differs from its account's
     * @throws AccountNotPostableException if an account is not {@code ACTIVE}
     */
    public PostTransferResult post(PostTransferCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        long startNanos = System.nanoTime();
        try {
            PostTransferResult result = doPost(command);
            if (result.replayed()) {
                replayedCounter.increment();
            } else {
                postedCounter.increment();
            }
            return result;
        } catch (RuntimeException e) {
            rejectedCounter.increment();
            throw e;
        } finally {
            postingTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }

    private PostTransferResult doPost(PostTransferCommand command) {
        // 1. In-memory validation. No lock, no connection, no Redis — the cheapest possible rejection.
        validateDoubleEntry(command);

        // 2 & 3. Fingerprint, then the idempotency fast path. A replay is answered here, before any
        // lock is taken, because a client retry storm on one key must not queue on that key's accounts.
        RequestFingerprint fingerprint = RequestFingerprint.of(command.canonicalForm());
        PostTransferResult replay = findReplay(command, fingerprint);
        if (replay != null) {
            return replay;
        }

        Set<UUID> involvedAccounts = command.involvedAccountIds();

        // 4. Distributed lock over every involved account, acquired in sorted order by the
        // implementation. Explicit wait and lease: never block indefinitely, never hold forever.
        try (AccountLockManager.LockHandle lock =
                     lockManager.lockAll(involvedAccounts, lockWaitTime, lockLeaseTime)) {

            // 5. One transaction: header, journal entries, and the outbox row commit together or not
            // at all. Invoked through the proxy — see the `self` field.
            PersistedPosting persisted = self.getObject().persistPosting(command, fingerprint);

            // 6. Authoritative post-commit balances. Read after the commit rather than inside it:
            // the balance columns are written by a database trigger, so a value read inside the
            // transaction from Hibernate's persistence context would be the pre-posting one.
            Map<UUID, Money> balancesAfter = refreshBalances(involvedAccounts);

            // Detect a lease that expired while we were working. It cannot be prevented, and the
            // ledger is still correct because PostgreSQL enforced the invariants — but exclusivity
            // was lost and that must not pass silently.
            if (!lock.stillHeld()) {
                log.error("lock lease expired during posting of transaction {}; accounts {} were "
                                + "briefly unprotected. The commit is still valid, but increase "
                                + "apex.ledger.locking.lease-time.",
                        persisted.transactionId(), involvedAccounts);
            }

            return new PostTransferResult(
                    persisted.transactionId(),
                    persisted.journalEntryIds(),
                    balancesAfter,
                    persisted.postedAt(),
                    false);
        }
    }

    /**
     * Persists the header, the journal entries and the outbox event in a single transaction.
     *
     * <p>Atomicity across these three is the entire point. If the outbox row could commit separately
     * from the entries, the system would either publish an event for a transfer that rolled back, or
     * commit a transfer whose event never reaches Kafka — and downstream balances would diverge from the
     * ledger with nothing failing loudly.
     *
     * <p>Public and invoked through the Spring proxy, never as {@code this.persistPosting(...)}.
     *
     * <p>Note the ordering inside the transaction: the header is written first, so the
     * {@code uq_transactions_idempotency_key} violation for a concurrent duplicate happens before any
     * journal entry is inserted.
     */
    @Transactional
    public PersistedPosting persistPosting(PostTransferCommand command,
                                           RequestFingerprint fingerprint) {
        Instant now = clock.instant();
        // Default the business date HERE, after the fingerprint was taken in doPost. Defaulting it in
        // the command would put a server-generated value into the fingerprint and break retries.
        Instant effectiveAt = command.effectiveAt() == null ? now : command.effectiveAt();

        validateAccounts(command);

        Transaction header = command.kind().requiresReversedTransaction()
                ? Transaction.reversalOf(command.reversesTransactionId(), command.idempotencyKey(),
                fingerprint, command.reference(), command.description(), effectiveAt,
                now, command.createdBy())
                : Transaction.of(command.idempotencyKey(), fingerprint, command.kind(),
                command.reference(), command.description(), effectiveAt, now,
                command.createdBy());

        List<JournalEntry> entries = new ArrayList<>(command.legs().size());
        int sequence = 0;
        for (PostTransferCommand.Leg leg : command.legs()) {
            entries.add(JournalEntry.of(header.getId(), leg.accountId(), sequence++,
                    leg.direction(), leg.amount(), now));
        }

        TransactionSettledEvent event = buildEvent(header, entries, now);
        OutboxEvent outboxEvent = OutboxEvent.pending(
                AGGREGATE_TYPE,
                header.getId(),
                TransactionSettledEvent.eventType(),
                journalEntriesTopic,
                // Keyed by transaction, not by account: a transaction can touch many accounts, and one
                // record cannot be keyed by all of them. Downstream consumers that need per-account
                // ordering must re-key onto an account-partitioned topic; see the class notes in
                // TransactionSettledEvent.
                header.getId().toString(),
                serialize(event),
                serialize(Map.of("eventType", TransactionSettledEvent.eventType(),
                        "aggregateType", AGGREGATE_TYPE)),
                now,
                now);

        try {
            // The idempotency guard wraps the writes so a concurrent duplicate surfaces as a typed
            // conflict rather than a raw integrity violation.
            return idempotencyGuard.execute(command.idempotencyKey(), fingerprint, () -> {
                transactions.save(header);
                // saveAll so the postings of one transaction go out as a JDBC batch. Safe only because
                // the double-entry check is a DEFERRED constraint trigger: it runs at COMMIT, so the
                // batch is never observed mid-insert as unbalanced.
                journalEntries.saveAll(entries);
                outbox.save(outboxEvent);
                return new PersistedPosting(
                        header.getId(),
                        entries.stream().map(JournalEntry::getId).toList(),
                        outboxEvent.getEventId(),
                        now);
            });
        } catch (RuntimeException e) {
            // Turn constraint violations — overdraft, unbalanced, currency mismatch, frozen account —
            // into typed domain exceptions. Anything unrecognised propagates untouched.
            throw constraintTranslator.translate(e, header.getId());
        }
    }

    /** What a successful persist produced. */
    public record PersistedPosting(UUID transactionId,
                                   List<UUID> journalEntryIds,
                                   UUID outboxEventId,
                                   Instant postedAt) {
        public PersistedPosting {
            journalEntryIds = List.copyOf(journalEntryIds);
        }
    }

    // ------------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------------

    /**
     * The double-entry check: debits must equal credits, independently in every currency.
     *
     * <p>Per currency, not in aggregate. A rule that only required the grand total to be zero would
     * happily accept debiting 100 USD and crediting 100 JPY. A genuine FX posting books through an FX
     * position account so that each currency balances on its own.
     *
     * <p>This duplicates {@code apex_assert_transaction_balanced()}, deliberately. The database check is
     * the authority — it cannot be bypassed — but it fires at COMMIT and reports a constraint name. This
     * one runs before any I/O and can say exactly which currency is out and by how much.
     */
    private void validateDoubleEntry(PostTransferCommand command) {
        Map<CurrencyCode, BigDecimal> netByCurrency = new LinkedHashMap<>();
        for (PostTransferCommand.Leg leg : command.legs()) {
            Money amount = leg.amount();
            BigDecimal signed = amount.amount()
                    .multiply(BigDecimal.valueOf(leg.direction().signum()));
            netByCurrency.merge(amount.currency(), signed, BigDecimal::add);
        }

        List<String> imbalances = new ArrayList<>();
        netByCurrency.forEach((currency, net) -> {
            if (net.signum() != 0) {
                imbalances.add("%s is out by %s".formatted(currency, net.toPlainString()));
            }
        });

        if (!imbalances.isEmpty()) {
            throw UnbalancedTransactionException.beforePersisting(
                    "debits do not equal credits: " + String.join("; ", imbalances));
        }
    }

    /**
     * Loads every involved account and checks what the database would reject anyway, so the caller gets
     * a precise error instead of a constraint name.
     *
     * <p>Both checks are also enforced in the database — the account status by
     * {@code apex_apply_entry_to_account_balance()}, the currency by the composite foreign key
     * {@code fk_journal_entries_account_currency}. Those are the authority; these produce better
     * diagnostics.
     */
    private void validateAccounts(PostTransferCommand command) {
        Map<UUID, Account> loaded = new HashMap<>();
        for (UUID accountId : command.involvedAccountIds()) {
            Account account = accounts.findById(accountId)
                    .orElseThrow(() -> new AccountNotPostableException(
                            "account %s does not exist".formatted(accountId)));
            if (!account.canPost()) {
                throw new AccountNotPostableException(
                        "account %s is %s and cannot accept postings"
                                .formatted(account.getAccountNumber(), account.getStatus()));
            }
            loaded.put(accountId, account);
        }

        for (PostTransferCommand.Leg leg : command.legs()) {
            Account account = loaded.get(leg.accountId());
            CurrencyCode legCurrency = leg.amount().currency();
            if (!account.getCurrency().equals(legCurrency)) {
                throw new CurrencyMismatchException(account.getCurrency(), legCurrency);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Idempotency
    // ------------------------------------------------------------------------

    /**
     * Returns a replay result when this exact request was already posted, or {@code null} to proceed.
     *
     * <p>A replay is a <em>success</em>, not an error: that is what idempotency means to a client that
     * retried after a timeout. Key reuse with a different payload is a different matter and is rethrown
     * — answering it with the original transaction would tell the caller a transfer succeeded that the
     * ledger never performed.
     */
    private PostTransferResult findReplay(PostTransferCommand command,
                                          RequestFingerprint fingerprint) {
        try {
            idempotencyGuard.assertUnused(command.idempotencyKey(), fingerprint);
            return null;
        } catch (IdempotencyConflictException e) {
            if (!e.isBenignReplay()) {
                throw e;
            }
            UUID existingId = e.existingTransactionId().orElseThrow(
                    () -> new IllegalStateException(
                            "a benign replay must identify the original transaction", e));
            Instant postedAt = transactions.findById(existingId)
                    .map(Transaction::getCreatedAt)
                    .orElseThrow(() -> new IllegalStateException(
                            "transaction %s owns idempotency key '%s' but could not be loaded"
                                    .formatted(existingId, command.idempotencyKey()), e));
            log.debug("returning replay of transaction {} for idempotency key '{}'",
                    existingId, command.idempotencyKey());
            return PostTransferResult.replayOf(existingId, postedAt);
        }
    }

    // ------------------------------------------------------------------------
    // Balance cache
    // ------------------------------------------------------------------------

    /**
     * Re-reads the committed balances and writes them through to the cache.
     *
     * <p>Called after COMMIT and while the lock is still held, so writes for one account cannot be
     * reordered against a concurrent posting's writes.
     *
     * <p>A failure here must not fail the posting: the money has moved and the event is staged. The
     * cache entry is evicted instead, so the next read repopulates from PostgreSQL.
     */
    private Map<UUID, Money> refreshBalances(Set<UUID> accountIds) {
        Map<UUID, Money> balances = new LinkedHashMap<>();
        for (UUID accountId : accountIds) {
            try {
                Account account = accounts.findById(accountId).orElse(null);
                if (account == null) {
                    // Impossible: a foreign key guarantees the row exists. Defensive only.
                    balanceProjection.evict(accountId);
                    continue;
                }
                Money balance = account.getBalance();
                balances.put(accountId, balance);
                BigDecimal fence = account.getTotalDebits().amount()
                        .add(account.getTotalCredits().amount());
                balanceProjection.recordCommittedBalance(accountId, balance, fence);
            } catch (RuntimeException e) {
                log.warn("could not refresh the cached balance for account {} after a committed "
                        + "posting; evicting so the next read repopulates", accountId, e);
                balanceProjection.evict(accountId);
            }
        }
        return balances;
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    private TransactionSettledEvent buildEvent(Transaction header,
                                               List<JournalEntry> entries,
                                               Instant postedAt) {
        List<TransactionSettledEvent.Entry> eventEntries = entries.stream()
                .map(entry -> new TransactionSettledEvent.Entry(
                        entry.getId(),
                        entry.getAccountId(),
                        entry.getEntrySequence(),
                        entry.getDirection(),
                        entry.getAmount().amount(),
                        entry.getCurrency().code()))
                .toList();

        return new TransactionSettledEvent(
                UUID.randomUUID(),
                header.getId(),
                header.getKind().name(),
                header.getIdempotencyKey().value(),
                header.getReference().orElse(null),
                header.getEffectiveAt(),
                postedAt,
                header.getCreatedBy(),
                header.getReversesTransactionId().orElse(null),
                eventEntries);
    }

    /**
     * Serialises an outbox payload.
     *
     * <p>Uses the application {@code ObjectMapper}, which {@code application.yml} configures with
     * {@code WRITE_BIGDECIMAL_AS_PLAIN} — so an amount is never emitted as {@code 1E+3} into a record
     * that consumers will replay from an indefinitely-retained topic.
     *
     * <p>A serialisation failure is a programming error, not a runtime condition: the event types are
     * plain records under our control. Failing loudly beats writing an unparseable outbox row that the
     * relay would retry until it is abandoned.
     */
    private String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "could not serialise the outbox payload of type %s"
                            .formatted(payload.getClass().getName()), e);
        }
    }
}
