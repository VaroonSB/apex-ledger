package com.apex.ledger.domain.model;

/**
 * Why a transaction was recorded.
 *
 * <p>Note what is absent: there is no {@code PENDING} or {@code FAILED}. A transaction row exists
 * only if it committed, and a committed transaction is never revised — it is offset by a
 * {@link #REVERSAL}. That is what lets the {@code transactions} table be append-only.
 */
public enum TransactionKind {

    /** Movement of value between accounts. The ordinary case. */
    TRANSFER(false),

    /** Operator-initiated correction that is not offsetting a specific prior transaction. */
    ADJUSTMENT(false),

    /** Offsets exactly one prior transaction, which it must name. */
    REVERSAL(true);

    private final boolean requiresReversedTransaction;

    TransactionKind(boolean requiresReversedTransaction) {
        this.requiresReversedTransaction = requiresReversedTransaction;
    }

    /**
     * Whether this kind must reference the transaction it offsets.
     *
     * <p>Enforced in the database by {@code ck_transactions_reversal_link}, which is an equality
     * between the two conditions — so a REVERSAL without a link and a TRANSFER with one are both
     * rejected.
     */
    public boolean requiresReversedTransaction() {
        return requiresReversedTransaction;
    }
}
