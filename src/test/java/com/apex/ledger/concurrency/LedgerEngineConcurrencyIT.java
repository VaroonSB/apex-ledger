package com.apex.ledger.concurrency;

import com.apex.ledger.application.port.in.PostTransferCommand;
import com.apex.ledger.application.port.in.PostTransferResult;
import com.apex.ledger.application.port.out.AccountBalanceProjection;
import com.apex.ledger.application.port.out.AccountLockManager;
import com.apex.ledger.application.service.LedgerEngineService;
import com.apex.ledger.domain.exception.AccountLockTimeoutException;
import com.apex.ledger.domain.exception.AccountNotPostableException;
import com.apex.ledger.domain.exception.CurrencyMismatchException;
import com.apex.ledger.domain.exception.IdempotencyConflictException;
import com.apex.ledger.domain.exception.InsufficientFundsException;
import com.apex.ledger.domain.exception.UnbalancedTransactionException;
import com.apex.ledger.domain.model.AccountStatus;
import com.apex.ledger.domain.model.AccountType;
import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.Direction;
import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.domain.model.OutboxStatus;
import com.apex.ledger.domain.model.TransactionKind;
import com.apex.ledger.infrastructure.cache.AccountBalanceCache;
import com.apex.ledger.infrastructure.persistence.entity.Account;
import com.apex.ledger.infrastructure.persistence.entity.OutboxEvent;
import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import com.apex.ledger.infrastructure.persistence.repository.JournalEntryRepository;
import com.apex.ledger.infrastructure.persistence.repository.OutboxEventRepository;
import com.apex.ledger.infrastructure.persistence.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Verifies the Phase 3 concurrency engine against real PostgreSQL and Redis.
 *
 * <p>Covers what only a running stack can show: that the Redisson lock serialises contended postings
 * instead of rejecting them, that a total lock order prevents opposing transfers from deadlocking, that
 * a lock held elsewhere produces a bounded, typed timeout, that the transaction/entries/outbox triple
 * commits atomically, and that the balance cache reads through, writes through, and discards
 * out-of-order writes.
 *
 * <p>Every concurrent case runs on {@code newVirtualThreadPerTaskExecutor}, so the assertions exercise
 * the deployed threading model rather than a platform-thread approximation.
 *
 * <p>Named {@code *IT} so failsafe runs it during {@code mvn verify}; it needs a container runtime.
 * Both containers are wired by {@code @ServiceConnection}, which contributes to
 * {@code RedisConnectionDetails} — the same abstraction {@code RedissonConfig} reads. That is what
 * keeps Redisson and Lettuce pointed at the same Redis here; had the config read
 * {@code spring.data.redis.host} directly, Redisson would talk to localhost while Lettuce talked to the
 * container, and the locks would appear to work while protecting nothing.
 */
@Testcontainers
@SpringBootTest
class LedgerEngineConcurrencyIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16.14-alpine")
                    .withDatabaseName("apex_ledger")
                    .withUsername("apex")
                    .withPassword("apex_local_test");

    /**
     * Plain GenericContainer rather than a Redis-specific module: Spring Boot's
     * RedisContainerConnectionDetailsFactory matches any container whose image name is {@code redis},
     * so no third-party Testcontainers module is needed. Pinned to the same image as
     * docker-compose.yml, and configured noeviction to match — an evicted idempotency key or lease
     * would be a silent correctness change, so the test environment must not differ.
     */
    @Container
    @ServiceConnection
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.10-alpine"))
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--maxmemory", "256mb",
                            "--maxmemory-policy", "noeviction", "--save", "");


    private static final CurrencyCode USD = CurrencyCode.of("USD");

    @Autowired LedgerEngineService engine;
    @Autowired AccountLockManager lockManager;
    @Autowired AccountBalanceProjection balances;
    @Autowired AccountBalanceCache cache;
    @Autowired AccountRepository accounts;
    @Autowired TransactionRepository transactions;
    @Autowired JournalEntryRepository entries;
    @Autowired OutboxEventRepository outbox;
    @Autowired TransactionTemplate tx;
    @Autowired com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    private String uniq() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private Account open(AccountType type, Money floor) {
        return tx.execute(s -> accounts.save(
                Account.open("ACC-" + uniq(), "T " + uniq(), type, USD, floor, Instant.now())));
    }

    /**
     * A balanced two-leg posting, named by what it does to each account rather than by a
     * from/to metaphor — which is ambiguous in double-entry and led to a wrong test here.
     */
    private PostTransferCommand posting(Account debited, Account credited, String amount, String key) {
        return new PostTransferCommand(
                IdempotencyKey.of(key), TransactionKind.TRANSFER,
                List.of(new PostTransferCommand.Leg(debited.getId(), Direction.DEBIT, Money.of(amount, USD)),
                        new PostTransferCommand.Leg(credited.getId(), Direction.CREDIT, Money.of(amount, USD))),
                "ref-" + key, "test transfer", Instant.now(), "tester", null);
    }

    /** Deposit: debit Cash (asset up), credit Customer (liability up). Customer ends up with `amount`. */
    private Account fundedCustomer(String amount) {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, Money.zero(USD));
        engine.post(posting(cash, customer, amount, "fund-" + uniq()));
        return customer;
    }

    // ------------------------------------------------------------ 1. happy path

    @Test
    void posts_transaction_entries_and_outbox_event_atomically() throws Exception {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);

        PostTransferResult result = engine.post(posting(cash, customer, "100.00", "k-" + uniq()));

        assertThat(result.replayed()).isFalse();
        assertThat(result.journalEntryIds()).hasSize(2);
        assertThat(transactions.findById(result.transactionId())).isPresent();
        assertThat(entries.findByTransactionIdOrderByEntrySequenceAsc(result.transactionId()))
                .hasSize(2);

        // All three writes landed in one transaction.
        OutboxEvent event = outbox.findStalled(Instant.now().plusSeconds(60), 100).stream()
                .filter(e -> e.getAggregateId().equals(result.transactionId()))
                .findFirst().orElseThrow();
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(event.getEventType()).isEqualTo("JournalEntryPosted");
        assertThat(event.getTopic()).isEqualTo("apex.ledger.journal-entries.v1");
        assertThat(event.getPartitionKey()).isEqualTo(result.transactionId().toString());
        // Assert on the parsed value, not the raw text: the payload column is jsonb, which reparses
        // and canonicalises the document, so key order and whitespace are NOT preserved. What must
        // survive is the numeric precision — jsonb keeps a JSON number as `numeric`, so a money
        // amount neither loses its scale nor turns into a double.
        com.fasterxml.jackson.databind.JsonNode amount = objectMapper.readTree(event.getPayload())
                .get("entries").get(0).get("amount");
        assertThat(amount.isNumber()).isTrue();
        assertThat(amount.decimalValue()).isEqualByComparingTo("100.00");
        assertThat(amount.decimalValue().scale()).isEqualTo(2);
        assertThat(event.getPayload()).doesNotContain("E+");

        // Balances returned by the engine agree with the database projection.
        assertThat(result.balancesAfter().get(cash.getId())).isEqualTo(Money.of("100.00", USD));
        assertThat(result.balancesAfter().get(customer.getId())).isEqualTo(Money.of("100.00", USD));
    }

    // ------------------------------------------------------ 2. validation, pre-I/O

    @Test
    void unbalanced_command_is_rejected_before_any_write() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "k-" + uniq();

        PostTransferCommand unbalanced = new PostTransferCommand(
                IdempotencyKey.of(key), TransactionKind.TRANSFER,
                List.of(new PostTransferCommand.Leg(cash.getId(), Direction.DEBIT, Money.of("100.00", USD)),
                        new PostTransferCommand.Leg(customer.getId(), Direction.CREDIT, Money.of("60.00", USD))),
                null, null, Instant.now(), "tester", null);

        assertThatThrownBy(() -> engine.post(unbalanced))
                .isInstanceOf(UnbalancedTransactionException.class)
                .hasMessageContaining("USD is out by 40.00");

        // Nothing persisted, and crucially the idempotency key was NOT consumed, so the caller can
        // correct the request and resubmit with the same key.
        assertThat(transactions.existsByIdempotencyKey(IdempotencyKey.of(key))).isFalse();
    }

    @Test
    void mixed_currency_legs_that_net_to_zero_only_in_aggregate_are_rejected() {
        Account usd = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        PostTransferCommand crossCurrency = new PostTransferCommand(
                IdempotencyKey.of("k-" + uniq()), TransactionKind.TRANSFER,
                List.of(new PostTransferCommand.Leg(usd.getId(), Direction.DEBIT, Money.of("100.00", USD)),
                        new PostTransferCommand.Leg(customer.getId(), Direction.CREDIT,
                                Money.of("100", CurrencyCode.of("JPY")))),
                null, null, Instant.now(), "tester", null);

        // Each currency must balance on its own; a grand total of zero is not enough.
        assertThatThrownBy(() -> engine.post(crossCurrency))
                .isInstanceOf(UnbalancedTransactionException.class);
    }

    @Test
    void leg_currency_must_match_its_account() {
        Account usd = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        CurrencyCode jpy = CurrencyCode.of("JPY");
        PostTransferCommand wrongCurrency = new PostTransferCommand(
                IdempotencyKey.of("k-" + uniq()), TransactionKind.TRANSFER,
                List.of(new PostTransferCommand.Leg(usd.getId(), Direction.DEBIT, Money.of("100", jpy)),
                        new PostTransferCommand.Leg(customer.getId(), Direction.CREDIT, Money.of("100", jpy))),
                null, null, Instant.now(), "tester", null);

        assertThatThrownBy(() -> engine.post(wrongCurrency))
                .isInstanceOf(CurrencyMismatchException.class);
    }

    @Test
    void frozen_account_cannot_be_posted_to() {
        Account cash = open(AccountType.ASSET, null);
        Account frozen = open(AccountType.LIABILITY, null);
        tx.execute(s -> {
            Account managed = accounts.findById(frozen.getId()).orElseThrow();
            managed.freeze();
            return accounts.save(managed);
        });
        assertThat(accounts.findById(frozen.getId()).orElseThrow().getStatus())
                .isEqualTo(AccountStatus.FROZEN);

        assertThatThrownBy(() -> engine.post(posting(cash, frozen, "1.00", "k-" + uniq())))
                .isInstanceOf(AccountNotPostableException.class);
    }

    // -------------------------------------------------- 3. concurrency & locking

    @Test
    void concurrent_withdrawals_of_the_same_balance_yield_exactly_one_success() throws Exception {
        Account customer = fundedCustomer("100.00");
        Account cash = open(AccountType.ASSET, null);
        int racers = 8;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<String>> tasks = IntStream.range(0, racers)
                    .<Callable<String>>mapToObj(i -> () -> {
                        try {
                            // Withdrawal: debit Customer (liability down), credit Cash.
                            engine.post(posting(customer, cash, "100.00", "w-" + uniq()));
                            return "OK";
                        } catch (InsufficientFundsException e) {
                            return "INSUFFICIENT";
                        } catch (AccountLockTimeoutException e) {
                            return "LOCK_TIMEOUT";
                        } catch (RuntimeException e) {
                            return "OTHER:" + e.getClass().getSimpleName();
                        }
                    }).toList();
            List<String> outcomes = pool.invokeAll(tasks).stream().map(this::get).toList();

            assertThat(outcomes).filteredOn("OK"::equals).hasSize(1);
            // The lock serialises rather than rejects: losers are refused by the balance floor,
            // not by a lock timeout. A timeout here would mean waitTime is too tight.
            assertThat(outcomes).filteredOn("INSUFFICIENT"::equals).hasSize(racers - 1);
        }

        assertThat(accounts.findCurrentBalance(customer.getId()).orElseThrow())
                .isEqualByComparingTo("0.00");
    }

    @Test
    void opposing_transfers_between_the_same_two_accounts_do_not_deadlock() throws Exception {
        Account a = open(AccountType.ASSET, null);
        Account b = open(AccountType.ASSET, null);
        int rounds = 24;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<String>> tasks = IntStream.range(0, rounds)
                    .<Callable<String>>mapToObj(i -> () -> {
                        try {
                            // Alternating direction: without a total lock order these interleave into
                            // A-then-B versus B-then-A and deadlock until waitTime expires.
                            engine.post(i % 2 == 0
                                    ? posting(b, a, "1.00", "d-" + uniq())
                                    : posting(a, b, "1.00", "d-" + uniq()));
                            return "OK";
                        } catch (RuntimeException e) {
                            return e.getClass().getSimpleName();
                        }
                    }).toList();
            List<String> outcomes = pool.invokeAll(tasks).stream().map(this::get).toList();
            assertThat(outcomes).containsOnly("OK");
        }

        // Equal traffic each way nets to zero on both accounts.
        assertThat(accounts.findCurrentBalance(a.getId()).orElseThrow()).isEqualByComparingTo("0.00");
        assertThat(accounts.findCurrentBalance(b.getId()).orElseThrow()).isEqualByComparingTo("0.00");
        assertThat(entries.findCurrenciesThatDoNotBalance()).isEmpty();
    }

    @Test
    void lock_held_elsewhere_makes_a_posting_time_out_rather_than_block_forever() throws Exception {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);

        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<RuntimeException> holderFailure = new AtomicReference<>();

        // Acquire on a separate thread: Redisson ties ownership to the acquiring thread, so the lock
        // must be both taken and released there.
        Thread holder = Thread.ofVirtual().start(() -> {
            try (AccountLockManager.LockHandle handle = lockManager.lockAll(
                    List.of(cash.getId(), customer.getId()),
                    Duration.ofSeconds(1), Duration.ofSeconds(20))) {
                acquired.countDown();
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                holderFailure.set(e);
                acquired.countDown();
            }
        });

        assertThat(acquired.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(holderFailure.get()).isNull();
        try {
            long start = System.nanoTime();
            assertThatThrownBy(() -> engine.post(posting(cash, customer, "1.00", "k-" + uniq())))
                    .isInstanceOf(AccountLockTimeoutException.class)
                    .satisfies(e -> {
                        AccountLockTimeoutException timeout = (AccountLockTimeoutException) e;
                        assertThat(timeout.isRetryable()).isTrue();
                        assertThat(timeout.errorCode()).isEqualTo("ACCOUNT_LOCK_TIMEOUT");
                        // Locked in sorted order, which is what prevents deadlock.
                        assertThat(timeout.accountIds())
                                .isSorted()
                                .containsExactlyInAnyOrder(cash.getId(), customer.getId());
                    });
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
            // Bounded by the configured 2s wait, not indefinite.
            assertThat(elapsed).isLessThan(Duration.ofSeconds(8));
        } finally {
            release.countDown();
            holder.join(Duration.ofSeconds(10));
        }

        // The lock is free again and the same posting now succeeds.
        assertThat(engine.post(posting(cash, customer, "1.00", "k-" + uniq())).replayed()).isFalse();
    }

    // ------------------------------------------------------------ 4. idempotency

    @Test
    void identical_resubmission_returns_the_original_outcome_without_double_posting() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "k-" + uniq();
        PostTransferCommand command = posting(cash, customer, "42.00", key);

        PostTransferResult first = engine.post(command);
        PostTransferResult replay = engine.post(command);

        assertThat(first.replayed()).isFalse();
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.transactionId()).isEqualTo(first.transactionId());

        // Exactly one posting reached the ledger.
        assertThat(entries.findByTransactionIdOrderByEntrySequenceAsc(first.transactionId()))
                .hasSize(2);
        assertThat(accounts.findCurrentBalance(cash.getId()).orElseThrow())
                .isEqualByComparingTo("42.00");
    }

    @Test
    void same_key_with_a_different_amount_is_a_hard_conflict() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "k-" + uniq();
        engine.post(posting(cash, customer, "10.00", key));

        assertThatThrownBy(() -> engine.post(posting(cash, customer, "999.00", key)))
                .isInstanceOf(IdempotencyConflictException.class)
                .satisfies(e -> assertThat(((IdempotencyConflictException) e).reason()).isEqualTo(
                        IdempotencyConflictException.Reason.KEY_REUSED_WITH_DIFFERENT_PAYLOAD));
    }

    @Test
    void leg_order_does_not_change_the_fingerprint() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "k-" + uniq();

        PostTransferResult first = engine.post(posting(cash, customer, "7.00", key));

        // Same transfer, legs reversed. Canonicalisation sorts them, so this is a replay — not a
        // spurious "key reused with a different payload" conflict.
        PostTransferCommand reordered = new PostTransferCommand(
                IdempotencyKey.of(key), TransactionKind.TRANSFER,
                List.of(new PostTransferCommand.Leg(customer.getId(), Direction.CREDIT, Money.of("7.00", USD)),
                        new PostTransferCommand.Leg(cash.getId(), Direction.DEBIT, Money.of("7.00", USD))),
                "ref-" + key, "test transfer", first.postedAt(), "tester", null);
        // effectiveAt participates in the fingerprint, so align it with the original.
        PostTransferCommand aligned = new PostTransferCommand(
                reordered.idempotencyKey(), reordered.kind(), reordered.legs(),
                reordered.reference(), reordered.description(),
                transactions.findById(first.transactionId()).orElseThrow().getEffectiveAt(),
                reordered.createdBy(), null);

        PostTransferResult replay = engine.post(aligned);
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.transactionId()).isEqualTo(first.transactionId());
    }

    @Test
    void concurrent_submissions_of_one_key_post_exactly_once() throws Exception {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "k-race-" + uniq();
        PostTransferCommand command = posting(cash, customer, "5.00", key);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<String>> tasks = IntStream.range(0, 8)
                    .<Callable<String>>mapToObj(i -> () -> {
                        try {
                            return engine.post(command).replayed() ? "REPLAY" : "POSTED";
                        } catch (RuntimeException e) {
                            return e.getClass().getSimpleName();
                        }
                    }).toList();
            List<String> outcomes = pool.invokeAll(tasks).stream().map(this::get).toList();
            assertThat(outcomes).filteredOn("POSTED"::equals).hasSize(1);
        }
        assertThat(accounts.findCurrentBalance(cash.getId()).orElseThrow())
                .isEqualByComparingTo("5.00");
    }

    // ----------------------------------------------------------- 5. balance cache

    @Test
    void cache_is_written_through_on_commit_and_read_through_after_eviction() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        engine.post(posting(cash, customer, "25.00", "k-" + uniq()));

        // Write-through: the value is in Redis without anyone having read it.
        assertThat(cache.rawEntry(cash.getId())).isPresent();
        assertThat(balances.findBalance(cash.getId())).contains(Money.of("25.00", USD));

        // Read-through: after eviction the next read repopulates from PostgreSQL.
        balances.evict(cash.getId());
        assertThat(cache.rawEntry(cash.getId())).isEmpty();
        assertThat(balances.findBalance(cash.getId())).contains(Money.of("25.00", USD));
        assertThat(cache.rawEntry(cash.getId())).isPresent();
    }

    @Test
    void an_out_of_order_cache_write_is_discarded() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        engine.post(posting(cash, customer, "50.00", "k-" + uniq()));

        Account current = accounts.findById(cash.getId()).orElseThrow();
        BigDecimal fence = AccountBalanceCache.fenceOf(current);

        // A slow writer carrying an older fence must not overwrite the newer balance.
        balances.recordCommittedBalance(cash.getId(), Money.of("999.00", USD),
                fence.subtract(BigDecimal.ONE));
        assertThat(balances.findBalance(cash.getId())).contains(Money.of("50.00", USD));

        // An equal-or-newer fence is accepted.
        balances.recordCommittedBalance(cash.getId(), Money.of("77.00", USD),
                fence.add(BigDecimal.ONE));
        assertThat(balances.findBalance(cash.getId())).contains(Money.of("77.00", USD));

        // Leave the cache consistent with the ledger for the invariant test below.
        balances.evict(cash.getId());
    }

    @Test
    void balance_of_an_unknown_account_is_empty_rather_than_an_error() {
        assertThat(balances.findBalance(UUID.randomUUID())).isEmpty();
    }

    // -------------------------------------------------------------- 6. invariants

    @Test
    void ledger_still_balances_globally() {
        assertThat(entries.findCurrenciesThatDoNotBalance()).isEmpty();
        assertThat(accounts.findAccountsWithDriftedBalanceProjection()).isEmpty();
    }

    @Test
    void engine_runs_on_virtual_threads() throws Exception {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Boolean> onVirtual = pool.submit(() -> {
                engine.post(posting(cash, customer, "1.00", "k-" + uniq()));
                return Thread.currentThread().isVirtual();
            });
            assertThat(onVirtual.get(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private <T> T get(Future<T> future) {
        try {
            return future.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            return fail("task failed", e);
        }
    }
}
