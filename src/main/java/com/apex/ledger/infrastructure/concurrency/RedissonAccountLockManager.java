package com.apex.ledger.infrastructure.concurrency;

import com.apex.ledger.application.port.out.AccountLockManager;
import com.apex.ledger.config.ApexLedgerProperties;
import com.apex.ledger.domain.exception.AccountLockInterruptedException;
import com.apex.ledger.domain.exception.AccountLockTimeoutException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Redisson-backed {@link AccountLockManager}.
 *
 * <h2>Deadlock avoidance by total ordering</h2>
 *
 * <p>Account ids are sorted before any lock is taken. Without that, a transfer A→B and a concurrent
 * transfer B→A would each hold one lock and wait for the other, and both would sit there until their
 * wait times expired — turning an ordinary pair of transfers into two failures plus latency. Sorting
 * imposes a global order, which makes a hold-and-wait cycle impossible.
 *
 * <p>Redisson's {@code getMultiLock} acquires in the order given and does <em>not</em> sort, so the
 * ordering has to happen here. A {@link TreeSet} does double duty: it sorts and it collapses a
 * duplicate account id, which would otherwise make Redisson try to lock the same key twice.
 *
 * <h2>Why MultiLock rather than acquiring one at a time</h2>
 *
 * <p>{@code RedissonMultiLock} handles the partial-acquisition case: if the third of three locks times
 * out, it releases the two it already holds before returning false. Hand-rolling that loop means
 * hand-rolling the unwind, and a missed unwind path leaves accounts locked until their lease expires.
 *
 * <p>One documented quirk to be aware of when reading Redisson: with both a wait time and a lease
 * time, {@code RedissonMultiLock} acquires the individual locks with an internal lease of
 * {@code waitTime * 2} and then re-applies the requested lease to each once it holds them all. The
 * effective lease after acquisition is the one requested here.
 *
 * <h2>Virtual threads</h2>
 *
 * <p>{@code tryLock} blocks the calling thread on a Netty future and a {@link java.util.concurrent.Semaphore};
 * both park through {@code AbstractQueuedSynchronizer}, so a waiting virtual thread yields its carrier
 * instead of pinning it. Nothing in this class holds a monitor across that wait — there is no
 * {@code synchronized} here at all, which is the property that matters for carrier pinning.
 *
 * <p>Every lock is taken with an <strong>explicit lease</strong>. That is deliberate beyond the
 * obvious safety argument: a lease of {@code -1} would engage Redisson's watchdog, which schedules a
 * renewal task per held lock keyed by {@code Thread#threadId()}. With a virtual thread per request,
 * thread ids are effectively unbounded, so watchdog state would grow with traffic rather than with
 * concurrency.
 */
@Component
public class RedissonAccountLockManager implements AccountLockManager {

    private static final Logger log = LoggerFactory.getLogger(RedissonAccountLockManager.class);

    private final RedissonClient redisson;
    private final String keyPrefix;
    private final Timer acquisitionTimer;
    private final Counter timeoutCounter;
    private final Counter leaseExpiredCounter;

    public RedissonAccountLockManager(RedissonClient redisson,
                                      ApexLedgerProperties properties,
                                      MeterRegistry meterRegistry) {
        this.redisson = Objects.requireNonNull(redisson, "redisson must not be null");
        this.keyPrefix = properties.locking().keyPrefix();

        this.acquisitionTimer = Timer.builder("apex.ledger.lock.acquisition")
                .description("Time spent acquiring the distributed account lock set")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.timeoutCounter = Counter.builder("apex.ledger.lock.timeout")
                .description("Lock acquisitions abandoned after exceeding the wait time")
                .register(meterRegistry);
        // A non-zero rate here means leases are expiring under their own holders, so the mutual
        // exclusion the engine assumes is not actually in force. Alert on it.
        this.leaseExpiredCounter = Counter.builder("apex.ledger.lock.lease.expired")
                .description("Locks found no longer held at release time, indicating lease expiry "
                        + "while the guarded work was still running")
                .register(meterRegistry);
    }

    @Override
    public LockHandle lockAll(Collection<UUID> accountIds, Duration waitTime, Duration leaseTime) {
        Objects.requireNonNull(accountIds, "accountIds must not be null");
        Objects.requireNonNull(waitTime, "waitTime must not be null");
        Objects.requireNonNull(leaseTime, "leaseTime must not be null");
        if (accountIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "cannot lock an empty account set; a posting always involves at least two accounts");
        }
        if (leaseTime.isZero() || leaseTime.isNegative()) {
            throw new IllegalArgumentException(
                    ("leaseTime must be positive, got %s; an infinite lease would engage Redisson's "
                            + "watchdog and leave a crashed holder blocking the account forever")
                            .formatted(leaseTime));
        }

        // Sorted and de-duplicated: the total order that makes deadlock impossible.
        List<UUID> ordered = new ArrayList<>(new TreeSet<>(accountIds));
        ordered.sort(Comparator.naturalOrder());

        RLock lock = buildLock(ordered);

        long startNanos = System.nanoTime();
        boolean acquired;
        try {
            acquired = lock.tryLock(waitTime.toMillis(), leaseTime.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // Restore the flag before unwinding so shutdown is still observable upstream.
            Thread.currentThread().interrupt();
            throw new AccountLockInterruptedException(ordered, e);
        } finally {
            acquisitionTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }

        if (!acquired) {
            // MultiLock has already released anything it partially acquired.
            timeoutCounter.increment();
            log.debug("lock timeout after {} for accounts {}", waitTime, ordered);
            throw new AccountLockTimeoutException(ordered, waitTime);
        }

        log.debug("acquired lock for accounts {} (lease {})", ordered, leaseTime);
        return new RedissonLockHandle(lock, ordered);
    }

    /**
     * Builds the lock to acquire over an already-ordered account set.
     *
     * <p>A single account uses the plain {@code RLock} rather than a one-element MultiLock, which
     * would add MultiLock's lease re-application round trip for no benefit.
     */
    private RLock buildLock(List<UUID> orderedAccountIds) {
        if (orderedAccountIds.size() == 1) {
            return redisson.getLock(lockKey(orderedAccountIds.get(0)));
        }
        RLock[] locks = orderedAccountIds.stream()
                .map(this::lockKey)
                .map(redisson::getLock)
                .toArray(RLock[]::new);
        return redisson.getMultiLock(locks);
    }

    private String lockKey(UUID accountId) {
        return keyPrefix + accountId;
    }

    /**
     * Handle over an acquired Redisson lock.
     *
     * <p>{@code close()} never throws. A failure to release is logged and counted, because throwing
     * from a try-with-resources' implicit close would replace the exception that is already unwinding
     * — hiding the real cause of a failed posting behind a lock-release detail. The lease guarantees
     * the lock is eventually freed regardless.
     */
    private final class RedissonLockHandle implements LockHandle {

        private final RLock lock;
        private final List<UUID> accountIds;
        private boolean released;

        private RedissonLockHandle(RLock lock, List<UUID> accountIds) {
            this.lock = lock;
            this.accountIds = List.copyOf(accountIds);
        }

        @Override
        public List<UUID> accountIds() {
            return accountIds;
        }

        @Override
        public boolean stillHeld() {
            if (released) {
                return false;
            }
            // isHeldByCurrentThread, not isLocked: another node holding it is precisely the condition
            // we are trying to detect, and isLocked would report true in exactly that case.
            return lock.isHeldByCurrentThread();
        }

        @Override
        public void close() {
            if (released) {
                return;
            }
            released = true;
            try {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    log.debug("released lock for accounts {}", accountIds);
                } else {
                    // The lease expired while the guarded work was still running. The posting may
                    // have committed anyway — PostgreSQL is the authority — but exclusivity was lost,
                    // so this must be visible.
                    leaseExpiredCounter.increment();
                    log.error("lock for accounts {} was no longer held at release time: the lease "
                                    + "expired while the posting was still running, so another node "
                                    + "may have posted concurrently. Increase "
                                    + "apex.ledger.locking.lease-time.",
                            accountIds);
                }
            } catch (RuntimeException e) {
                // Swallowed on purpose: see the class comment. The lease bounds the consequence.
                log.error("failed to release the lock for accounts {}; it will be freed when the "
                        + "lease expires", accountIds, e);
            }
        }
    }
}
