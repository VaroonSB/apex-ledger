package com.apex.ledger.domain.exception;

/**
 * Raised when an account cannot accept postings because it is not {@code ACTIVE}.
 *
 * <p>Enforced in the database by {@code apex_apply_entry_to_account_balance()}, which only folds an
 * entry into a row whose status is {@code ACTIVE} and raises otherwise. Checking it there rather than
 * in the application closes the window between reading a status and writing the entry.
 */
public final class AccountNotPostableException extends LedgerException {

    private static final long serialVersionUID = 1L;

    public AccountNotPostableException(String message, Throwable cause) {
        super(message, cause);
    }

    public AccountNotPostableException(String message) {
        super(message);
    }

    @Override
    public String errorCode() {
        return "ACCOUNT_NOT_POSTABLE";
    }
}
