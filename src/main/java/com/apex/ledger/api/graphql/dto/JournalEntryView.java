package com.apex.ledger.api.graphql.dto;

import com.apex.ledger.domain.model.Direction;
import com.apex.ledger.infrastructure.persistence.entity.JournalEntry;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The {@code JournalEntry} GraphQL type.
 *
 * <p>{@code transactionId} is carried but not exposed in the schema: it is the key the
 * {@code transaction} batch loader groups by. The schema exposes the resolved {@code transaction}
 * object instead, so a client never has to make a second round trip to turn an id into a transaction.
 */
public record JournalEntryView(
        UUID id,
        UUID accountId,
        UUID transactionId,
        int entrySequence,
        Direction direction,
        BigDecimal amount,
        String currency,
        BigDecimal signedAmount,
        Instant createdAt
) {

    public static JournalEntryView from(JournalEntry entry) {
        return new JournalEntryView(
                entry.getId(),
                entry.getAccountId(),
                entry.getTransactionId(),
                entry.getEntrySequence(),
                entry.getDirection(),
                entry.getAmount().amount(),
                entry.getCurrency().code(),
                entry.getSignedAmount().amount(),
                entry.getCreatedAt());
    }
}
