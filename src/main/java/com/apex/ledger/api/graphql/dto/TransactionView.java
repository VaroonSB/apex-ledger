package com.apex.ledger.api.graphql.dto;

import com.apex.ledger.domain.model.TransactionKind;
import com.apex.ledger.infrastructure.persistence.entity.Transaction;

import java.time.Instant;
import java.util.UUID;

/**
 * The {@code Transaction} GraphQL type.
 *
 * <p>The {@code entries} field is absent from this record deliberately: it is resolved separately by a
 * batch loader. Holding the entries here would force every query that returns a transaction to load
 * them, whether the client asked for the field or not — the exact over-fetching GraphQL exists to
 * avoid.
 */
public record TransactionView(
        UUID id,
        TransactionKind kind,
        String idempotencyKey,
        String reference,
        String description,
        Instant effectiveAt,
        Instant createdAt,
        String createdBy,
        UUID reversesTransactionId
) {

    public static TransactionView from(Transaction transaction) {
        return new TransactionView(
                transaction.getId(),
                transaction.getKind(),
                transaction.getIdempotencyKey().value(),
                transaction.getReference().orElse(null),
                transaction.getDescription().orElse(null),
                transaction.getEffectiveAt(),
                transaction.getCreatedAt(),
                transaction.getCreatedBy(),
                transaction.getReversesTransactionId().orElse(null));
    }
}
