package com.apex.ledger.api.graphql.error;

import com.apex.ledger.api.graphql.support.EntryCursor;
import com.apex.ledger.domain.exception.AccountLockInterruptedException;
import com.apex.ledger.domain.exception.AccountLockTimeoutException;
import com.apex.ledger.domain.exception.AccountNotPostableException;
import com.apex.ledger.domain.exception.CurrencyMismatchException;
import com.apex.ledger.domain.exception.IdempotencyConflictException;
import com.apex.ledger.domain.exception.ImmutableLedgerViolationException;
import com.apex.ledger.domain.exception.InsufficientFundsException;
import com.apex.ledger.domain.exception.LedgerException;
import com.apex.ledger.domain.exception.UnbalancedTransactionException;
import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.schema.DataFetchingEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.graphql.execution.DataFetcherExceptionResolverAdapter;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps domain exceptions to GraphQL errors.
 *
 * <h2>What a client is given, and why</h2>
 *
 * <p>Each error carries three things beyond its message:
 *
 * <ul>
 *   <li>{@code errorType} — one of Spring GraphQL's standard classifications, so generic tooling that
 *       knows nothing about this API can still tell a client mistake from a server fault.
 *   <li>{@code extensions.errorCode} — the precise, stable domain code. This is what clients should
 *       branch on. Messages are written for humans and will be reworded; codes are a contract.
 *   <li>{@code extensions.retryable} — whether repeating the identical request may succeed. Without
 *       it, callers guess, and the usual guess is to retry everything, which turns a rejected
 *       overdraft into a hot loop.
 * </ul>
 *
 * <p>The standard classification set has no "conflict" or "service busy" member, so a lock timeout and
 * an idempotency conflict both land under a broader type; {@code errorCode} is what disambiguates them.
 * Inventing custom {@code errorType} values would break the interoperability the field exists for.
 *
 * <h2>Anything not listed here</h2>
 *
 * <p>Returning {@code null} from {@link #resolveToSingleError} lets Spring GraphQL apply its default:
 * an {@code INTERNAL_ERROR} with a generic message and the detail confined to the server log. That is
 * the correct handling for an unexpected exception — a stack trace or a SQL fragment in a GraphQL
 * response is an information leak, and a ledger's error text can easily name accounts and amounts.
 *
 * <p>So the resolver is deliberately an allow-list. A new domain exception surfaces as a generic
 * internal error until it is added here, which is the safe direction to fail.
 */
@Component
public class LedgerExceptionResolver extends DataFetcherExceptionResolverAdapter {

    private static final Logger log = LoggerFactory.getLogger(LedgerExceptionResolver.class);

    public LedgerExceptionResolver() {
        // Make the resolver's own mapping failures visible instead of silently degrading to a generic
        // internal error.
        setThreadLocalContextAware(false);
    }

    @Override
    protected GraphQLError resolveToSingleError(Throwable exception, DataFetchingEnvironment env) {

        if (exception instanceof IdempotencyConflictException conflict) {
            // A benign replay never reaches here: the engine answers it as a success with
            // replayed=true. This is key reuse with different content, or a concurrent submission.
            Map<String, Object> extensions = baseExtensions(conflict, conflict.isRetryable());
            conflict.existingTransactionId()
                    .ifPresent(id -> extensions.put("existingTransactionId", id.toString()));
            extensions.put("idempotencyKey", conflict.key().value());
            return error(exception, env, ErrorType.BAD_REQUEST, conflict.getMessage(), extensions);
        }

        if (exception instanceof AccountLockTimeoutException timeout) {
            // Load shedding, not a client mistake: the accounts are busy and nothing was written.
            // Logged at info rather than warn — under contention this is the system working.
            log.info("shedding load: {}", timeout.getMessage());
            Map<String, Object> extensions = baseExtensions(timeout, true);
            extensions.put("waitTimeMillis", timeout.waitTime().toMillis());
            extensions.put("accountIds", timeout.accountIds().stream().map(Object::toString).toList());
            return error(exception, env, ErrorType.INTERNAL_ERROR, timeout.getMessage(), extensions);
        }

        if (exception instanceof AccountLockInterruptedException interrupted) {
            // Shutdown in progress. Retryable against another instance.
            return error(exception, env, ErrorType.INTERNAL_ERROR, interrupted.getMessage(),
                    baseExtensions(interrupted, true));
        }

        if (exception instanceof InsufficientFundsException insufficient) {
            Map<String, Object> extensions = baseExtensions(insufficient, false);
            if (insufficient.accountId() != null) {
                extensions.put("accountId", insufficient.accountId().toString());
            }
            return error(exception, env, ErrorType.BAD_REQUEST, insufficient.getMessage(), extensions);
        }

        if (exception instanceof UnbalancedTransactionException unbalanced) {
            return error(exception, env, ErrorType.BAD_REQUEST, unbalanced.getMessage(),
                    baseExtensions(unbalanced, false));
        }

        if (exception instanceof CurrencyMismatchException mismatch) {
            Map<String, Object> extensions = baseExtensions(mismatch, false);
            mismatch.expected().ifPresent(code -> extensions.put("expectedCurrency", code.code()));
            mismatch.actual().ifPresent(code -> extensions.put("actualCurrency", code.code()));
            return error(exception, env, ErrorType.BAD_REQUEST, mismatch.getMessage(), extensions);
        }

        if (exception instanceof AccountNotPostableException notPostable) {
            return error(exception, env, ErrorType.BAD_REQUEST, notPostable.getMessage(),
                    baseExtensions(notPostable, false));
        }

        if (exception instanceof ImmutableLedgerViolationException violation) {
            // Reaching this means something tried to rewrite history and the database stopped it.
            // A defect, not a client error: log loudly, tell the client nothing specific.
            log.error("append-only violation surfaced through the API", violation);
            return error(exception, env, ErrorType.INTERNAL_ERROR,
                    "The ledger rejected an attempt to modify recorded history.",
                    baseExtensions(violation, false));
        }

        if (exception instanceof EntryCursor.InvalidCursorException invalidCursor) {
            return error(exception, env, ErrorType.BAD_REQUEST, invalidCursor.getMessage(),
                    extensions("INVALID_CURSOR", false));
        }

        if (exception instanceof IllegalArgumentException illegalArgument) {
            // Value-object validation: a malformed currency code, an amount finer than the currency
            // permits, a blank idempotency key. These messages are written for callers and name only
            // the offending input, so they are safe to return.
            return error(exception, env, ErrorType.BAD_REQUEST, illegalArgument.getMessage(),
                    extensions("INVALID_INPUT", false));
        }

        if (exception instanceof LedgerException ledgerException) {
            // A domain exception added since this resolver was written. Its code is still stable and
            // safe to expose, but flag the omission so the mapping gets completed.
            log.warn("unmapped LedgerException subtype {}; returning its error code without a "
                            + "specific classification",
                    ledgerException.getClass().getSimpleName());
            return error(exception, env, ErrorType.BAD_REQUEST, ledgerException.getMessage(),
                    baseExtensions(ledgerException, false));
        }

        // Not ours: let Spring GraphQL produce a generic INTERNAL_ERROR and keep the detail in the log.
        return null;
    }

    private GraphQLError error(Throwable exception, DataFetchingEnvironment env, ErrorType errorType,
                               String message, Map<String, Object> extensions) {
        return GraphqlErrorBuilder.newError(env)
                .errorType(errorType)
                .message(message)
                .extensions(extensions)
                .build();
    }

    private Map<String, Object> baseExtensions(LedgerException exception, boolean retryable) {
        return extensions(exception.errorCode(), retryable);
    }

    private Map<String, Object> extensions(String errorCode, boolean retryable) {
        // LinkedHashMap so the extension order in a response is stable and diffable in tests.
        Map<String, Object> extensions = new LinkedHashMap<>();
        extensions.put("errorCode", errorCode);
        extensions.put("retryable", retryable);
        return extensions;
    }
}
