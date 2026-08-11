package com.apex.ledger.infrastructure.persistence;

/**
 * Names of the database constraints the application translates into typed domain exceptions.
 *
 * <p>These strings are a contract with {@code V1__init_schema.sql}. A constraint renamed in a
 * migration without being renamed here does not break the build — it silently degrades a precise
 * domain exception into a generic {@code DataIntegrityViolationException}. Keeping them in one class
 * makes the coupling visible instead of scattering literals through catch blocks.
 */
public final class DatabaseConstraints {

    /** Duplicate submission: {@code transactions.idempotency_key} is already taken. */
    public static final String UQ_TRANSACTIONS_IDEMPOTENCY_KEY = "uq_transactions_idempotency_key";

    /** A transaction may be reversed at most once. */
    public static final String UQ_TRANSACTIONS_REVERSES = "uq_transactions_reverses";

    /** Overdraft: the posting would push the account below {@code minimum_balance}. */
    public static final String CK_ACCOUNTS_MINIMUM_BALANCE = "ck_accounts_minimum_balance";

    /** Synthetic name raised by the balance trigger for a non-ACTIVE account. */
    public static final String CK_ACCOUNTS_STATUS_POSTABLE = "ck_accounts_status_postable";

    /** Synthetic name raised by the deferred double-entry balance trigger. */
    public static final String CK_JOURNAL_ENTRIES_BALANCED = "ck_journal_entries_balanced";

    /** An entry's currency must equal its account's currency. */
    public static final String FK_JOURNAL_ENTRIES_ACCOUNT_CURRENCY =
            "fk_journal_entries_account_currency";

    /** The same posting cannot be written twice within one transaction. */
    public static final String UQ_JOURNAL_ENTRIES_TRANSACTION_SEQUENCE =
            "uq_journal_entries_transaction_sequence";

    /**
     * SQLSTATE raised by {@code apex_forbid_mutation()} when something tries to mutate an
     * append-only table. {@code 0A000} is {@code feature_not_supported}, chosen because no ordinary
     * constraint uses it, which makes the append-only violation unambiguous.
     */
    public static final String SQLSTATE_APPEND_ONLY_VIOLATION = "0A000";

    /** SQLSTATE {@code unique_violation}. */
    public static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    /** SQLSTATE {@code check_violation}, also used by the raising triggers. */
    public static final String SQLSTATE_CHECK_VIOLATION = "23514";

    /** SQLSTATE {@code foreign_key_violation}. */
    public static final String SQLSTATE_FOREIGN_KEY_VIOLATION = "23503";

    private DatabaseConstraints() {
        throw new AssertionError("no instances");
    }
}
