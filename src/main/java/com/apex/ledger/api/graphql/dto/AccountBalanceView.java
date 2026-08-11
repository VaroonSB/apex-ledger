package com.apex.ledger.api.graphql.dto;

import com.apex.ledger.domain.model.AccountStatus;
import com.apex.ledger.domain.model.AccountType;
import com.apex.ledger.infrastructure.persistence.entity.Account;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The {@code AccountBalance} GraphQL type.
 *
 * <p>A view, not the entity. Entity accessors return {@code Money} and {@code CurrencyCode}, which have
 * no GraphQL representation, and exposing the entity would also publish its mutators and its JPA
 * identity semantics into the API contract. Mapping here means the schema can evolve independently of
 * the persistence model.
 *
 * <p>{@code asOf} is part of the contract on purpose: a balance is a value from the past, and telling
 * the client when it was observed is what makes that visible rather than implied.
 */
public record AccountBalanceView(
        UUID accountId,
        String accountNumber,
        AccountType accountType,
        AccountStatus status,
        String currency,
        BigDecimal balance,
        BigDecimal totalDebits,
        BigDecimal totalCredits,
        BigDecimal minimumBalance,
        Instant asOf
) {

    public static AccountBalanceView from(Account account, Instant asOf) {
        return new AccountBalanceView(
                account.getId(),
                account.getAccountNumber(),
                account.getAccountType(),
                account.getStatus(),
                account.getCurrency().code(),
                account.getBalance().amount(),
                account.getTotalDebits().amount(),
                account.getTotalCredits().amount(),
                account.getMinimumBalance().map(money -> money.amount()).orElse(null),
                asOf);
    }
}
