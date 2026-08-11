package com.apex.ledger.application.port.in;

import com.apex.ledger.domain.model.Money;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Outcome of a successful posting.
 *
 * <p>{@code replayed} distinguishes a fresh posting from an idempotent replay of an earlier identical
 * submission. Both are successes from the caller's point of view — that is the entire promise of
 * idempotency — but the distinction matters for metrics and for anyone reading an audit trail.
 */
public record PostTransferResult(
        UUID transactionId,
        List<UUID> journalEntryIds,
        Map<UUID, Money> balancesAfter,
        Instant postedAt,
        boolean replayed
) {
    public PostTransferResult {
        journalEntryIds = List.copyOf(journalEntryIds);
        balancesAfter = Map.copyOf(balancesAfter);
    }

    public static PostTransferResult replayOf(UUID transactionId, Instant postedAt) {
        return new PostTransferResult(transactionId, List.of(), Map.of(), postedAt, true);
    }
}
