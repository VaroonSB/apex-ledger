package com.apex.ledger.domain.exception;

/**
 * Base type for every failure that represents a violated ledger rule, as opposed to an
 * infrastructure fault.
 *
 * <p>Unchecked on purpose. These are not conditions a caller recovers from mid-operation: the
 * posting transaction is finished, and the only sensible responses are to report the failure or to
 * retry the whole submission.
 *
 * <p>{@link #errorCode()} is the stable, machine-readable discriminator. GraphQL error extensions
 * carry it so clients can branch on the code rather than pattern-matching messages, which are for
 * humans and will change.
 */
public abstract class LedgerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected LedgerException(String message) {
        super(message);
    }

    protected LedgerException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Stable error code, e.g. {@code IDEMPOTENCY_CONFLICT}. Safe to expose to clients. */
    public abstract String errorCode();
}
