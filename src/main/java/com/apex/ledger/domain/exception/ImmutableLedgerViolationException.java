package com.apex.ledger.domain.exception;

/**
 * Raised when something attempted to modify or delete recorded ledger history.
 *
 * <p>Reaching this exception means a defence-in-depth layer did its job and an earlier one did not.
 * The application has no code path that updates {@code journal_entries} or {@code transactions}, the
 * entities are annotated {@code @Immutable} with every column {@code updatable = false}, and the
 * repositories do not expose delete operations. This is the translation of the database's
 * {@code apex_forbid_mutation()} trigger firing anyway — so it should be treated as a defect to
 * investigate, never as an expected error to handle.
 */
public final class ImmutableLedgerViolationException extends LedgerException {

    private static final long serialVersionUID = 1L;

    public ImmutableLedgerViolationException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String errorCode() {
        return "IMMUTABLE_LEDGER_VIOLATION";
    }
}
