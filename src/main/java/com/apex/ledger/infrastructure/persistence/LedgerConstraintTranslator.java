package com.apex.ledger.infrastructure.persistence;

import com.apex.ledger.domain.exception.AccountNotPostableException;
import com.apex.ledger.domain.exception.CurrencyMismatchException;
import com.apex.ledger.domain.exception.ImmutableLedgerViolationException;
import com.apex.ledger.domain.exception.InsufficientFundsException;
import com.apex.ledger.domain.exception.LedgerException;
import com.apex.ledger.domain.exception.UnbalancedTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

/**
 * Translates database constraint violations into typed {@link LedgerException}s.
 *
 * <p>The database is the authority on every ledger invariant — that is the whole design — but it
 * reports a breach as a {@link DataAccessException} naming a constraint. Without translation, the API
 * layer would have to string-match SQL error text to tell an overdraft from an unbalanced entry, and
 * every caller would need to know the schema. This class is the single place that knowledge lives.
 *
 * <p>Deliberately <em>not</em> registered as a Spring {@code PersistenceExceptionTranslator}. Those
 * run inside the repository proxy, where the failing transaction is already doomed and the enclosing
 * transaction template has not yet unwound; throwing a domain exception from there obscures the
 * rollback. It is invoked explicitly by the service layer instead, which keeps the translation point
 * visible in the code that owns the operation.
 *
 * <p>Anything unrecognised is returned unchanged. A genuine infrastructure fault — a lost connection,
 * a serialisation failure — must not be dressed up as a business rule violation, because the caller's
 * correct response differs: retry the one, do not retry the other.
 */
@Component
public class LedgerConstraintTranslator {

    /**
     * Maps {@code throwable} to a domain exception when it names a known ledger constraint.
     *
     * @param transactionId the transaction being written, used to give the exception context; may be
     *     {@code null} when unknown
     * @return the translated exception, or {@code throwable} itself if it is not a recognised ledger
     *     invariant
     */
    public RuntimeException translate(RuntimeException throwable, UUID transactionId) {
        // Checked before constraint names: the append-only trigger raises a distinctive SQLSTATE and
        // does not name a constraint, and it signals a defect rather than a business outcome.
        if (ConstraintViolations.isAppendOnlyViolation(throwable)) {
            return new ImmutableLedgerViolationException(
                    "attempted to modify or delete recorded ledger history; "
                            + "ledger tables are append-only and this indicates a defect",
                    throwable);
        }

        Optional<String> constraint = ConstraintViolations.constraintName(throwable);
        if (constraint.isEmpty()) {
            return throwable;
        }

        return switch (constraint.get()) {
            case DatabaseConstraints.CK_ACCOUNTS_MINIMUM_BALANCE ->
                    new InsufficientFundsException(null, throwable);

            case DatabaseConstraints.CK_ACCOUNTS_STATUS_POSTABLE ->
                    new AccountNotPostableException(
                            "posting rejected: the target account is not ACTIVE", throwable);

            case DatabaseConstraints.CK_JOURNAL_ENTRIES_BALANCED ->
                    new UnbalancedTransactionException(
                            transactionId,
                            "entries do not sum to zero in every currency, or fewer than two "
                                    + "entries were supplied",
                            throwable);

            // The foreign key proves entry currency equals account currency; when it fires, the
            // violation names the row but not the two codes, so neither is reported.
            case DatabaseConstraints.FK_JOURNAL_ENTRIES_ACCOUNT_CURRENCY ->
                    new CurrencyMismatchException(
                            "posting rejected: the entry's currency does not match its account's "
                                    + "currency", throwable);

            default -> throwable;
        };
    }

    /** Convenience overload for operations not tied to a specific transaction. */
    public RuntimeException translate(RuntimeException throwable) {
        return translate(throwable, null);
    }
}
