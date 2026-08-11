package com.apex.ledger.domain.exception;

import com.apex.ledger.domain.model.IdempotencyKey;

import java.util.Optional;
import java.util.UUID;

/**
 * Raised when an idempotency key has already been consumed.
 *
 * <p>Carries a {@link Reason} because the two cases demand opposite handling, and collapsing them
 * into one error is how idempotency implementations go wrong:
 *
 * <ul>
 *   <li>{@link Reason#DUPLICATE_REQUEST} — same key, same request. A benign retry. The original
 *       transaction is identified by {@link #existingTransactionId()}, so an API layer can return the
 *       first outcome and present the operation as successful. Surfacing this as an error to the
 *       caller would make honest retries look like failures.
 *   <li>{@link Reason#KEY_REUSED_WITH_DIFFERENT_PAYLOAD} — same key, different request. A client
 *       defect or a key collision. This must be reported as a hard conflict: returning the original
 *       transaction would tell the caller a transfer they never requested had succeeded.
 * </ul>
 *
 * <p>{@link #isBenignReplay()} exists so callers branch on intent rather than on the enum.
 */
public final class IdempotencyConflictException extends LedgerException {

    private static final long serialVersionUID = 1L;

    public enum Reason {

        /** Same key, same request fingerprint: a retry of an already-accepted submission. */
        DUPLICATE_REQUEST,

        /** Same key, different request fingerprint: the key was reused for other content. */
        KEY_REUSED_WITH_DIFFERENT_PAYLOAD,

        /**
         * The unique constraint fired but the winning row could not be read back — two requests
         * raced and the other side had not committed yet. Retryable, unlike the cases above.
         */
        CONCURRENT_SUBMISSION
    }

    private final IdempotencyKey key;
    private final Reason reason;
    private final UUID existingTransactionId;

    private IdempotencyConflictException(
            String message, IdempotencyKey key, Reason reason, UUID existingTransactionId,
            Throwable cause) {
        super(message, cause);
        this.key = key;
        this.reason = reason;
        this.existingTransactionId = existingTransactionId;
    }

    public static IdempotencyConflictException duplicate(
            IdempotencyKey key, UUID existingTransactionId) {
        return new IdempotencyConflictException(
                ("idempotency key '%s' was already used by transaction %s with an identical "
                        + "request; this is a replay, not a new submission")
                        .formatted(key, existingTransactionId),
                key, Reason.DUPLICATE_REQUEST, existingTransactionId, null);
    }

    public static IdempotencyConflictException payloadMismatch(
            IdempotencyKey key, UUID existingTransactionId) {
        return new IdempotencyConflictException(
                ("idempotency key '%s' was already used by transaction %s with a DIFFERENT "
                        + "request; reusing a key for different content is not permitted")
                        .formatted(key, existingTransactionId),
                key, Reason.KEY_REUSED_WITH_DIFFERENT_PAYLOAD, existingTransactionId, null);
    }

    public static IdempotencyConflictException concurrentSubmission(
            IdempotencyKey key, Throwable cause) {
        return new IdempotencyConflictException(
                ("idempotency key '%s' was claimed concurrently by another in-flight request; "
                        + "retry to observe the committed outcome")
                        .formatted(key),
                key, Reason.CONCURRENT_SUBMISSION, null, cause);
    }

    public IdempotencyKey key() {
        return key;
    }

    public Reason reason() {
        return reason;
    }

    /** The transaction that already owns this key, when it was readable. */
    public Optional<UUID> existingTransactionId() {
        return Optional.ofNullable(existingTransactionId);
    }

    /**
     * True when the caller resent an identical request and the original outcome can safely be
     * returned in place of an error.
     */
    public boolean isBenignReplay() {
        return reason == Reason.DUPLICATE_REQUEST;
    }

    /** True when retrying the submission may succeed or yield a definitive answer. */
    public boolean isRetryable() {
        return reason == Reason.CONCURRENT_SUBMISSION;
    }

    @Override
    public String errorCode() {
        return switch (reason) {
            case DUPLICATE_REQUEST -> "IDEMPOTENT_REPLAY";
            case KEY_REUSED_WITH_DIFFERENT_PAYLOAD -> "IDEMPOTENCY_KEY_REUSED";
            case CONCURRENT_SUBMISSION -> "IDEMPOTENCY_CONCURRENT_SUBMISSION";
        };
    }
}
