package com.apex.ledger.domain.exception;

import com.apex.ledger.domain.model.CurrencyCode;

import java.util.Optional;

/**
 * Raised when an operation would mix currencies implicitly.
 *
 * <p>A ledger never converts as a side effect. Crossing currencies is an explicit FX posting routed
 * through a position account so that each currency still balances to zero on its own — which is what
 * {@code apex_assert_transaction_balanced()} verifies per currency.
 *
 * <p>Arises from two places, hence the optional currency detail. When the domain detects it — in
 * {@link com.apex.ledger.domain.model.Money#requireSameCurrency} — both currencies are known. When the
 * database detects it, through the composite foreign key
 * {@code fk_journal_entries_account_currency}, the violation identifies the offending row but not the
 * two codes, so they are absent.
 */
public final class CurrencyMismatchException extends LedgerException {

    private static final long serialVersionUID = 1L;

    private final CurrencyCode expected;
    private final CurrencyCode actual;

    public CurrencyMismatchException(CurrencyCode expected, CurrencyCode actual) {
        super("currency mismatch: expected %s but got %s; implicit conversion is never performed"
                .formatted(expected, actual));
        this.expected = expected;
        this.actual = actual;
    }

    /** For a mismatch detected by a database constraint, where the two codes are not reported. */
    public CurrencyMismatchException(String message, Throwable cause) {
        super(message, cause);
        this.expected = null;
        this.actual = null;
    }

    /** The currency the operation required, when known. */
    public Optional<CurrencyCode> expected() {
        return Optional.ofNullable(expected);
    }

    /** The currency actually supplied, when known. */
    public Optional<CurrencyCode> actual() {
        return Optional.ofNullable(actual);
    }

    @Override
    public String errorCode() {
        return "CURRENCY_MISMATCH";
    }
}
