package com.apex.ledger.domain.model;

/**
 * Which side of the ledger a posting lands on.
 *
 * <p>Amounts are always stored positive; this enum carries the sign. {@link #signum()} defines the
 * convention used by the double-entry invariant: a transaction is balanced when
 * {@code sum(signum * amount) == 0} for every currency it touches. The same expression appears in
 * {@code apex_assert_transaction_balanced()} in the V1 migration, where it is enforced.
 */
public enum Direction {

    DEBIT(1),
    CREDIT(-1);

    private final int signum;

    Direction(int signum) {
        this.signum = signum;
    }

    /** {@code +1} for a debit, {@code -1} for a credit. */
    public int signum() {
        return signum;
    }

    public Direction opposite() {
        return this == DEBIT ? CREDIT : DEBIT;
    }
}
