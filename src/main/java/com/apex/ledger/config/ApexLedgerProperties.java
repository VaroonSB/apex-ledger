package com.apex.ledger.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Binds the {@code apex.ledger.*} configuration tree declared in {@code application.yml}.
 *
 * <p>Constructor-bound records, so every value is final once the context starts and nothing can
 * mutate configuration at runtime. {@code @DefaultValue} supplies a working default for each nested
 * block, so an absent section binds to defaults rather than {@code null}.
 */
@Validated
@ConfigurationProperties(prefix = "apex.ledger")
public record ApexLedgerProperties(

        /** Reporting currency for consolidated positions. Never used for implicit conversion. */
        @NotBlank String baseCurrency,

        @NotNull @DefaultValue Idempotency idempotency,
        @NotNull @DefaultValue Locking locking,
        @NotNull @DefaultValue Topics topics,
        @NotNull @DefaultValue Cache cache
) {

    public record Idempotency(
            @NotBlank @DefaultValue("apex:idem:") String keyPrefix,
            @NotNull @DefaultValue("24h") Duration ttl
    ) {
        public Idempotency {
            if (ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException(
                        "apex.ledger.idempotency.ttl must be positive, got " + ttl);
            }
        }
    }

    /**
     * Distributed-lock timings.
     *
     * <p>These two durations are the whole safety/liveness trade-off of the locking layer, and the
     * relationship between them and the transaction timeout is what matters:
     *
     * <ul>
     *   <li>{@code waitTime} bounds how long a caller queues for a contended account before being
     *       rejected. Too long and requests pile up behind a hot account holding connections and
     *       virtual threads; too short and normal contention surfaces as spurious failures.
     *   <li>{@code leaseTime} bounds how long a crashed holder can block everyone else. It
     *       <strong>must exceed the maximum duration of the transaction it guards</strong>
     *       ({@code spring.transaction.default-timeout}, 10s). If the lease expires while the posting
     *       transaction is still running, a second node acquires the lock and posts concurrently —
     *       the classic fencing problem, since Redis cannot revoke a lease already handed out.
     *       {@link RedissonConfig} asserts this ordering at startup.
     * </ul>
     */
    public record Locking(
            @NotBlank @DefaultValue("apex:lock:account:") String keyPrefix,
            @NotNull @DefaultValue("15s") Duration leaseTime,
            @NotNull @DefaultValue("2s") Duration waitTime
    ) {
        public Locking {
            if (leaseTime.isNegative() || leaseTime.isZero()) {
                throw new IllegalArgumentException(
                        "apex.ledger.locking.lease-time must be positive, got " + leaseTime);
            }
            if (waitTime.isNegative()) {
                throw new IllegalArgumentException(
                        "apex.ledger.locking.wait-time must not be negative, got " + waitTime);
            }
            if (leaseTime.compareTo(waitTime) <= 0) {
                throw new IllegalArgumentException(
                        ("apex.ledger.locking.lease-time (%s) must exceed wait-time (%s); otherwise "
                                + "a lock can expire before a queued waiter has even acquired it")
                                .formatted(leaseTime, waitTime));
            }
        }
    }

    public record Topics(
            @NotBlank @DefaultValue("apex.ledger.journal-entries.v1") String journalEntries,
            @NotBlank @DefaultValue("apex.ledger.balance-projections.v1") String balanceProjections,
            @NotBlank @DefaultValue("apex.ledger.dlq.v1") String deadLetter
    ) {
    }

    /**
     * Balance-cache settings.
     *
     * <p>{@code ttl} is a correctness backstop, not a performance knob: it bounds how long a balance
     * can be wrong if a write-through is ever lost (a node dying between COMMIT and the cache write).
     * Keep it short enough that such a gap self-heals quickly.
     */
    public record Cache(
            @DefaultValue("true") boolean enabled,
            @NotBlank @DefaultValue("apex:balance:") String keyPrefix,
            @NotNull @DefaultValue("60s") Duration ttl
    ) {
        public Cache {
            if (ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException(
                        "apex.ledger.cache.ttl must be positive, got " + ttl);
            }
        }
    }
}
