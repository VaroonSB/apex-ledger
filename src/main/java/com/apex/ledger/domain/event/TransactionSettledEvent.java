package com.apex.ledger.domain.event;

import com.apex.ledger.domain.model.Direction;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Published when a transaction and its journal entries have been committed and are final.
 *
 * <p>"Settled" is the operative word: this event is emitted only from a committed transaction, via the
 * outbox, so it never describes a posting that later rolled back. There is no corresponding "pending"
 * or "failed" event — a failed posting leaves no row and therefore no event.
 *
 * <p>The outbound contract. Once consumers exist this shape can only be extended, never narrowed:
 * removing or retyping a field breaks every reader, and the Kafka topic retains history indefinitely
 * ({@code log.retention.hours=-1}), so old records in the old shape are replayable forever.
 *
 * <p>Amounts travel as {@link BigDecimal} with the currency alongside. Serialisation is configured for
 * {@code WRITE_BIGDECIMAL_AS_PLAIN}, so an amount is never emitted in scientific notation — a consumer
 * parsing {@code 1E+3} as a balance is the kind of defect that surfaces only in production, and only in
 * the accounting.
 *
 * <p>Carries no balances. Balances are a projection of the journal, and a consumer that needs one should
 * derive it from the entries or query the read side — embedding a balance snapshot in an event that may
 * be redelivered out of order invites consumers to treat a stale value as current.
 */
public record TransactionSettledEvent(
        UUID eventId,
        UUID transactionId,
        String kind,
        String idempotencyKey,
        String reference,
        Instant effectiveAt,
        Instant postedAt,
        String postedBy,
        UUID reversesTransactionId,
        List<Entry> entries
) {

    /** One posting, mirroring a {@code journal_entries} row. */
    public record Entry(
            UUID journalEntryId,
            UUID accountId,
            short entrySequence,
            Direction direction,
            BigDecimal amount,
            String currency
    ) {
    }

    public TransactionSettledEvent {
        entries = List.copyOf(entries);
    }

    /** Stable event-type discriminator carried in the outbox row and as a Kafka header. */
    public static String eventType() {
        return "TransactionSettled";
    }
}
