package com.apex.ledger.domain.exception;

import java.util.UUID;

/**
 * Raised when a posting would drive an account below its configured floor.
 *
 * <p>The authoritative check is the database constraint {@code ck_accounts_minimum_balance}, which is
 * evaluated inside the balance trigger's UPDATE. That placement is what makes overdraft protection
 * hold under concurrency: the UPDATE takes a row lock, so simultaneous withdrawals serialise and
 * each one is re-checked against the balance left by its predecessor. This exception is the typed
 * translation of that constraint violation.
 */
public final class InsufficientFundsException extends LedgerException {

    private static final long serialVersionUID = 1L;

    private final UUID accountId;

    public InsufficientFundsException(UUID accountId, Throwable cause) {
        super(("posting rejected: account %s would fall below its minimum balance")
                .formatted(accountId), cause);
        this.accountId = accountId;
    }

    public InsufficientFundsException(String message) {
        super(message);
        this.accountId = null;
    }

    /** The offending account, when the database error identified one; otherwise {@code null}. */
    public UUID accountId() {
        return accountId;
    }

    @Override
    public String errorCode() {
        return "INSUFFICIENT_FUNDS";
    }
}
