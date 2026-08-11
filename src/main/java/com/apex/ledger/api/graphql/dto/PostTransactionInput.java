package com.apex.ledger.api.graphql.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The {@code PostTransactionInput} GraphQL input type.
 *
 * <p>Bean Validation annotations catch shape errors — a blank key, a non-positive amount — before the
 * engine is entered, so the client gets a precise field-level message rather than a domain exception.
 * They are a convenience, not the guarantee: the ledger's invariants are enforced by database
 * constraints regardless of what reaches them.
 *
 * <p>See the schema documentation on {@code postTransaction} for what source and destination mean.
 * Briefly: source is CREDITED, destination is DEBITED, and whether that raises or lowers a balance
 * depends on the account's type.
 */
public record PostTransactionInput(
        @NotNull UUID sourceAccountId,
        @NotNull UUID destinationAccountId,
        @NotNull @Positive BigDecimal amount,
        @NotBlank @Size(min = 3, max = 3) String currency,
        @NotBlank @Size(max = 255) String idempotencyKey,
        @Size(max = 128) String reference,
        @Size(max = 512) String description,
        Instant effectiveAt
) {
}
