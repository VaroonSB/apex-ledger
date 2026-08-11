package com.apex.ledger.domain.model;

/**
 * The five classical account classifications, each with the side its balance naturally sits on.
 *
 * <p>The debit-normal set ({@code ASSET}, {@code EXPENSE}) is duplicated in SQL: the generated
 * {@code accounts.balance} column and the {@code ck_accounts_minimum_balance} constraint both
 * branch on {@code account_type IN ('ASSET','EXPENSE')}. Changing the mapping here without changing
 * the migration would put the Java view of a balance out of step with the database's, so the two
 * must be edited together.
 */
public enum AccountType {

    ASSET(Direction.DEBIT),
    LIABILITY(Direction.CREDIT),
    EQUITY(Direction.CREDIT),
    REVENUE(Direction.CREDIT),
    EXPENSE(Direction.DEBIT);

    private final Direction normalBalance;

    AccountType(Direction normalBalance) {
        this.normalBalance = normalBalance;
    }

    /** The side on which a positive balance for this account type accumulates. */
    public Direction normalBalance() {
        return normalBalance;
    }

    /** Whether a posting in {@code direction} increases this account's balance. */
    public boolean increasesWith(Direction direction) {
        return direction == normalBalance;
    }
}
