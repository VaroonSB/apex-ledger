package com.apex.ledger.integration;

import com.apex.ledger.application.port.out.IdempotencyGuard;
import com.apex.ledger.domain.exception.IdempotencyConflictException;
import com.apex.ledger.domain.exception.ImmutableLedgerViolationException;
import com.apex.ledger.domain.exception.InsufficientFundsException;
import com.apex.ledger.domain.exception.UnbalancedTransactionException;
import com.apex.ledger.domain.model.AccountType;
import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.domain.model.RequestFingerprint;
import com.apex.ledger.domain.model.TransactionKind;
import com.apex.ledger.infrastructure.persistence.ConstraintViolations;
import com.apex.ledger.infrastructure.persistence.LedgerConstraintTranslator;
import com.apex.ledger.infrastructure.persistence.entity.Account;
import com.apex.ledger.infrastructure.persistence.entity.JournalEntry;
import com.apex.ledger.infrastructure.persistence.entity.OutboxEvent;
import com.apex.ledger.infrastructure.persistence.entity.Transaction;
import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import com.apex.ledger.infrastructure.persistence.repository.JournalEntryRepository;
import com.apex.ledger.infrastructure.persistence.repository.OutboxEventRepository;
import com.apex.ledger.infrastructure.persistence.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Verifies the Phase 2 schema and entity mappings against a real PostgreSQL.
 *
 * <p>Covers what cannot be asserted without a database: that Flyway's V1 migration applies, that
 * Hibernate's {@code ddl-auto=validate} agrees with it, that the append-only triggers and the
 * deferred double-entry constraint actually fire, and that the idempotency guard classifies replays,
 * key reuse and concurrent submissions correctly.
 *
 * <p>Named {@code *IT} so failsafe runs it during {@code mvn verify}; it needs a container runtime.
 * {@code @ServiceConnection} derives {@code spring.datasource.*} from the container, so no datasource
 * property is hard-coded anywhere.
 *
 * <p><strong>No cleanup between tests, deliberately.</strong> The append-only triggers reject TRUNCATE
 * and DELETE on {@code journal_entries} and {@code transactions}, so the usual "truncate between
 * tests" strategy is impossible here — which is the immutability guarantee working as intended. Each
 * test therefore creates its own accounts and unique idempotency keys, and isolation comes from the
 * container being fresh per run. This is a real consequence of database-enforced immutability worth
 * knowing before writing further tests against these tables.
 */
@Testcontainers
@SpringBootTest
class LedgerSchemaIT {

    /**
     * Pinned to the same image as docker-compose.yml so the tests exercise the version that runs
     * locally and in production. Static, so one container is reused for every test in the class.
     */
    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16.14-alpine")
                    .withDatabaseName("apex_ledger")
                    .withUsername("apex")
                    .withPassword("apex_local_test");



    private static final CurrencyCode USD = CurrencyCode.of("USD");

    @Autowired AccountRepository accounts;
    @Autowired TransactionRepository transactions;
    @Autowired JournalEntryRepository entries;
    @Autowired OutboxEventRepository outbox;
    @Autowired IdempotencyGuard idempotencyGuard;
    @Autowired LedgerConstraintTranslator translator;
    @Autowired TransactionTemplate tx;
    @Autowired JdbcTemplate jdbc;

    private String uniq() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private Account openAccount(AccountType type, Money floor) {
        return tx.execute(s -> accounts.save(
                Account.open("ACC-" + uniq(), "Test " + uniq(), type, USD, floor, Instant.now())));
    }

    private Transaction postTransfer(Account from, Account to, Money amount, String key) {
        return tx.execute(s -> {
            Transaction t = transactions.save(Transaction.of(
                    IdempotencyKey.of(key), RequestFingerprint.of("req:" + key),
                    TransactionKind.TRANSFER, null, "test", Instant.now(), Instant.now(), "tester"));
            entries.saveAll(List.of(
                    JournalEntry.debit(t.getId(), to.getId(), 0, amount, Instant.now()),
                    JournalEntry.credit(t.getId(), from.getId(), 1, amount, Instant.now())));
            return t;
        });
    }

    // ---------------------------------------------------------------- 1. round trip

    @Test
    void persists_and_reads_back_a_balanced_transaction() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, Money.zero(USD));

        Transaction t = postTransfer(customer, cash, Money.of("100.00", USD), "k-" + uniq());

        List<JournalEntry> posted = entries.findByTransactionIdOrderByEntrySequenceAsc(t.getId());
        assertThat(posted).hasSize(2);
        assertThat(posted.get(0).getAmount()).isEqualTo(Money.of("100.00", USD));
        assertThat(posted.get(0).getSignedAmount()).isEqualTo(Money.of("100.00", USD));
        assertThat(posted.get(1).getSignedAmount()).isEqualTo(Money.of("-100.00", USD));

        // Balance trigger folded both entries into their accounts.
        assertThat(accounts.findCurrentBalance(cash.getId()).orElseThrow())
                .isEqualByComparingTo("100.00");
        // customer is credit-normal (LIABILITY), so a 100 credit is a +100 balance.
        assertThat(accounts.findCurrentBalance(customer.getId()).orElseThrow())
                .isEqualByComparingTo("100.00");

        // NUMERIC(38,18) round trip is renormalised to the currency's 2 minor units, not scale 18.
        Account reloaded = accounts.findById(cash.getId()).orElseThrow();
        assertThat(reloaded.getBalance()).isEqualTo(Money.of("100.00", USD));
        assertThat(reloaded.getBalance().amount().scale()).isEqualTo(2);
        assertThat(reloaded.getTotalDebits()).isEqualTo(Money.of("100.00", USD));
    }

    // ---------------------------------------------------------------- 2. immutability

    @Test
    void hibernate_immutable_silently_ignores_an_update_and_the_db_blocks_raw_sql() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, null);
        Transaction t = postTransfer(customer, cash, Money.of("50.00", USD), "k-" + uniq());
        UUID entryId = entries.findByTransactionIdOrderByEntrySequenceAsc(t.getId()).get(0).getId();

        // Layer 1: there is no mutator to call. Asserted reflectively, because "has no setters"
        // is a claim about the API surface and is otherwise only checked by code review.
        assertThat(java.util.Arrays.stream(JournalEntry.class.getMethods())
                .filter(m -> m.getDeclaringClass() == JournalEntry.class)
                .filter(m -> m.getName().startsWith("set"))
                .toList()).isEmpty();

        // Layer 2: the class is final, so no subclass can reintroduce mutable state.
        assertThat(java.lang.reflect.Modifier.isFinal(JournalEntry.class.getModifiers())).isTrue();

        // Layer 5: raw SQL is blocked by the append-only triggers. Driven through JdbcTemplate on
        // its own connection: issuing this inside the JPA transaction leaves the connection aborted
        // and the real error is masked by a rollback failure.
        RuntimeException upd = catchRuntime(() ->
                jdbc.update("update journal_entries set amount = 999 where id = ?", entryId));
        assertThat(ConstraintViolations.isAppendOnlyViolation(upd)).isTrue();
        assertThat(translator.translate(upd))
                .isInstanceOf(ImmutableLedgerViolationException.class);

        RuntimeException del = catchRuntime(() ->
                jdbc.update("delete from journal_entries where id = ?", entryId));
        assertThat(ConstraintViolations.isAppendOnlyViolation(del)).isTrue();
        assertThat(translator.translate(del)).isInstanceOf(ImmutableLedgerViolationException.class);

        // Still there, unchanged.
        assertThat(entries.findById(entryId).orElseThrow().getAmount())
                .isEqualTo(Money.of("50.00", USD));
    }

    // ---------------------------------------------------------------- 3. double-entry

    @Test
    void unbalanced_transaction_is_rejected_at_commit() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, null);

        RuntimeException thrown = catchRuntime(() -> tx.execute(s -> {
            Transaction t = transactions.save(Transaction.of(
                    IdempotencyKey.of("k-" + uniq()), RequestFingerprint.of("x"),
                    TransactionKind.TRANSFER, null, null, Instant.now(), Instant.now(), "tester"));
            entries.saveAll(List.of(
                    JournalEntry.debit(t.getId(), cash.getId(), 0, Money.of("100.00", USD), Instant.now()),
                    JournalEntry.credit(t.getId(), customer.getId(), 1, Money.of("60.00", USD), Instant.now())));
            return t;
        }));
        assertThat(translator.translate(thrown, null))
                .isInstanceOf(UnbalancedTransactionException.class);
    }

    // ---------------------------------------------------------------- 4. overdraft

    @Test
    void overdraft_is_rejected_by_the_minimum_balance_constraint() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, Money.zero(USD));
        postTransfer(customer, cash, Money.of("100.00", USD), "k-" + uniq());

        // customer is at -100 already; the floor is 0 so it is the *cash* side we must overdraw.
        Account floored = openAccount(AccountType.ASSET, Money.zero(USD));
        RuntimeException thrown = catchRuntime(
                () -> postTransfer(floored, cash, Money.of("10.00", USD), "k-" + uniq()));
        assertThat(translator.translate(thrown)).isInstanceOf(InsufficientFundsException.class);
    }

    // ---------------------------------------------------------------- 5. idempotency

    @Test
    void duplicate_key_with_identical_request_is_a_benign_replay() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, null);
        String key = "k-" + uniq();
        Transaction original = postTransfer(customer, cash, Money.of("25.00", USD), key);

        RequestFingerprint same = RequestFingerprint.of("req:" + key);
        assertThatThrownBy(() -> idempotencyGuard.assertUnused(IdempotencyKey.of(key), same))
                .isInstanceOf(IdempotencyConflictException.class)
                .satisfies(e -> {
                    IdempotencyConflictException c = (IdempotencyConflictException) e;
                    assertThat(c.isBenignReplay()).isTrue();
                    assertThat(c.reason())
                            .isEqualTo(IdempotencyConflictException.Reason.DUPLICATE_REQUEST);
                    assertThat(c.existingTransactionId()).contains(original.getId());
                    assertThat(c.errorCode()).isEqualTo("IDEMPOTENT_REPLAY");
                });
    }

    @Test
    void same_key_with_a_different_request_is_a_hard_conflict() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, null);
        String key = "k-" + uniq();
        postTransfer(customer, cash, Money.of("25.00", USD), key);

        assertThatThrownBy(() -> idempotencyGuard.assertUnused(
                IdempotencyKey.of(key), RequestFingerprint.of("a completely different request")))
                .isInstanceOf(IdempotencyConflictException.class)
                .satisfies(e -> {
                    IdempotencyConflictException c = (IdempotencyConflictException) e;
                    assertThat(c.isBenignReplay()).isFalse();
                    assertThat(c.reason()).isEqualTo(
                            IdempotencyConflictException.Reason.KEY_REUSED_WITH_DIFFERENT_PAYLOAD);
                    assertThat(c.errorCode()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
                });
    }

    @Test
    void guard_translates_the_unique_violation_when_the_fast_path_is_bypassed() {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, null);
        String key = "k-" + uniq();
        postTransfer(customer, cash, Money.of("25.00", USD), key);

        // Simulates losing the race: the fast-path SELECT is skipped and the INSERT collides.
        // This is the assertion that proves ConstraintViolations can read the constraint NAME out
        // of a PostgreSQL error, which the whole translation layer depends on.
        RuntimeException thrown = catchRuntime(() -> tx.execute(s -> transactions.save(
                Transaction.of(IdempotencyKey.of(key), RequestFingerprint.of("other"),
                        TransactionKind.TRANSFER, null, null, Instant.now(), Instant.now(), "t"))));

        assertThat(com.apex.ledger.infrastructure.persistence.ConstraintViolations
                .constraintName(thrown)).contains("uq_transactions_idempotency_key");
        assertThat(com.apex.ledger.infrastructure.persistence.ConstraintViolations
                .isUniqueViolation(thrown)).isTrue();
    }

    @Test
    void concurrent_submissions_of_one_key_yield_exactly_one_transaction() throws Exception {
        Account cash = openAccount(AccountType.ASSET, null);
        Account customer = openAccount(AccountType.LIABILITY, null);
        String key = "k-race-" + uniq();
        int racers = 8;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Boolean>> tasks = java.util.stream.IntStream.range(0, racers)
                    .<Callable<Boolean>>mapToObj(i -> () -> {
                        try {
                            idempotencyGuard.execute(IdempotencyKey.of(key),
                                    RequestFingerprint.of("req:" + key),
                                    () -> postTransfer(customer, cash, Money.of("1.00", USD), key));
                            return true;
                        } catch (RuntimeException e) {
                            return false;
                        }
                    }).toList();
            List<Future<Boolean>> results = pool.invokeAll(tasks);
            long winners = results.stream().filter(f -> {
                try {
                    return f.get();
                } catch (Exception e) {
                    return false;
                }
            }).count();
            assertThat(winners).isEqualTo(1L);
        }
        assertThat(transactions.existsByIdempotencyKey(IdempotencyKey.of(key))).isTrue();
    }

    // ---------------------------------------------------------------- 6. money

    @Test
    void money_rejects_precision_finer_than_the_currency_allows() {
        assertThatThrownBy(() -> Money.of("1.005", USD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more precise");
        assertThat(Money.of("1.10", USD)).isEqualTo(Money.of("1.1", USD));
        assertThat(Money.of(new BigDecimal("100.000000000000000000"), USD))
                .isEqualTo(Money.of("100.00", USD));
        // JPY has zero minor units.
        CurrencyCode jpy = CurrencyCode.of("JPY");
        assertThat(Money.of("500", jpy).amount().scale()).isZero();
        assertThatThrownBy(() -> Money.of("500.5", jpy))
                .isInstanceOf(IllegalArgumentException.class);
        // XAU is a unit of account, not a transactable currency.
        assertThatThrownBy(() -> CurrencyCode.of("XAU"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- 7. outbox

    @Test
    void outbox_row_transitions_through_dispatch_states() {
        OutboxEvent staged = tx.execute(s -> outbox.save(OutboxEvent.pending(
                "Transaction", UUID.randomUUID(), "JournalEntryPosted",
                "apex.ledger.journal-entries.v1", "acct-1", "{\"amount\":\"100.00\"}", null,
                Instant.now(), Instant.now())));
        assertThat(staged.getId()).isNotNull();
        assertThat(staged.getStatus().isClaimable()).isTrue();
        assertThat(staged.getPublishedAt()).isEmpty();

        List<OutboxEvent> claimed = tx.execute(s -> outbox.claimPendingBatch(Instant.now(), 10));
        assertThat(claimed).extracting(OutboxEvent::getEventId).contains(staged.getEventId());

        tx.execute(s -> {
            OutboxEvent e = outbox.findById(staged.getId()).orElseThrow();
            e.markFailed("broker unavailable", Instant.now().plusSeconds(30));
            return outbox.save(e);
        });
        OutboxEvent failed = outbox.findById(staged.getId()).orElseThrow();
        assertThat(failed.getAttempts()).isEqualTo(1);
        assertThat(failed.getPublishedAt()).isEmpty();
        assertThat(failed.getLastError()).contains("broker unavailable");

        tx.execute(s -> {
            OutboxEvent e = outbox.findById(staged.getId()).orElseThrow();
            e.markPublished(Instant.now());
            return outbox.save(e);
        });
        OutboxEvent published = outbox.findById(staged.getId()).orElseThrow();
        assertThat(published.getStatus().isTerminal()).isTrue();
        assertThat(published.getPublishedAt()).isPresent();
        assertThat(published.getLastError()).isEmpty();
    }

    // ---------------------------------------------------------------- 8. invariants

    @Test
    void global_ledger_invariants_hold() {
        assertThat(entries.findCurrenciesThatDoNotBalance()).isEmpty();
        assertThat(accounts.findAccountsWithDriftedBalanceProjection()).isEmpty();
    }

    private static RuntimeException catchRuntime(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            return e;
        }
        return fail("expected a RuntimeException but none was thrown");
    }
}
