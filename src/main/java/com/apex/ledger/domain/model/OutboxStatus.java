package com.apex.ledger.domain.model;

/**
 * Dispatch state of a transactional-outbox row.
 *
 * <p>This is delivery bookkeeping, not ledger history, which is why {@code outbox_events} is the one
 * mutable table in the schema.
 */
public enum OutboxStatus {

    /** Committed with the ledger change, not yet handed to Kafka. */
    PENDING(true, false),

    /** Acknowledged by the broker. Terminal. */
    PUBLISHED(false, true),

    /** Publication failed; eligible for retry once {@code available_at} passes. */
    FAILED(true, false),

    /** Retry budget exhausted. Terminal, and requires operator attention. */
    ABANDONED(false, true);

    private final boolean claimable;
    private final boolean terminal;

    OutboxStatus(boolean claimable, boolean terminal) {
        this.claimable = claimable;
        this.terminal = terminal;
    }

    /**
     * Whether the relay may pick this row up. Mirrors the predicate of the partial index
     * {@code idx_outbox_events_claimable}; keep the two in step or the relay loses its index.
     */
    public boolean isClaimable() {
        return claimable;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
