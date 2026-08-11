package com.apex.ledger.api.graphql.dto;

import java.util.List;

/**
 * The {@code PostTransactionPayload} GraphQL type.
 *
 * <p>{@code replayed} is surfaced rather than hidden. A replay is a success — that is what idempotency
 * promises a client that retried after a timeout — but a caller reconciling its own records needs to
 * know whether this call is what created the transaction.
 */
public record PostTransactionPayload(
        TransactionView transaction,
        boolean replayed,
        List<AccountBalanceView> balancesAfter
) {
    public PostTransactionPayload {
        balancesAfter = List.copyOf(balancesAfter);
    }
}
