package com.apex.ledger.domain.exception;

import java.util.UUID;

/**
 * Raised when a transaction's postings do not sum to zero in some currency, or when it has fewer
 * than two entries.
 *
 * <p>Also the translation target for the database's deferred constraint trigger
 * {@code trg_journal_entries_balanced}, which is the authoritative check. Because that trigger is
 * {@code DEFERRABLE INITIALLY DEFERRED} it fires at COMMIT, so this exception can surface from the
 * commit itself rather than from the statement that inserted the offending entry.
 */
public final class UnbalancedTransactionException extends LedgerException {

    private static final long serialVersionUID = 1L;

    private final UUID transactionId;

    public UnbalancedTransactionException(UUID transactionId, String detail) {
        super("%s does not satisfy double-entry: %s".formatted(describe(transactionId), detail));
        this.transactionId = transactionId;
    }

    /**
     * For the in-memory check that runs before anything is written, where no transaction id exists yet.
     */
    public static UnbalancedTransactionException beforePersisting(String detail) {
        return new UnbalancedTransactionException(null, detail);
    }

    private static String describe(UUID transactionId) {
        return transactionId == null ? "the submitted posting" : "transaction " + transactionId;
    }

    public UnbalancedTransactionException(UUID transactionId, String detail, Throwable cause) {
        super("%s does not satisfy double-entry: %s".formatted(describe(transactionId), detail),
                cause);
        this.transactionId = transactionId;
    }

    /** The offending transaction, or {@code null} when the check ran before anything was persisted. */
    public UUID transactionId() {
        return transactionId;
    }

    @Override
    public String errorCode() {
        return "UNBALANCED_TRANSACTION";
    }
}
