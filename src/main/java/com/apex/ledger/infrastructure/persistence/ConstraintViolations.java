package com.apex.ledger.infrastructure.persistence;

import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the constraint name and SQLSTATE from a database exception so callers can translate a
 * generic integrity violation into a typed domain exception.
 *
 * <p>Deliberately free of any PostgreSQL import. The JDBC driver is declared {@code runtime} scope in
 * the POM, so {@code org.postgresql.util.PSQLException} is not on the compile classpath. Everything
 * here works through {@link SQLException#getSQLState()}, Hibernate's dialect-driven constraint-name
 * extraction, and — for COMMIT-time failures where neither is available — the error message. See
 * {@link #constraintName(Throwable)} for why that last fallback is necessary.
 *
 * <p>Why constraint names at all: a {@link DataIntegrityViolationException} tells you something was
 * rejected but not what rule was broken. A duplicate idempotency key, an overdraft and a
 * currency mismatch all arrive as the same Spring exception type. The constraint name is the only
 * reliable discriminator, which is why {@link DatabaseConstraints} keeps those names in one place.
 */
public final class ConstraintViolations {

    private ConstraintViolations() {
        throw new AssertionError("no instances");
    }

    /**
     * Matches PostgreSQL's own phrasing, {@code violates ... constraint "name"}, which appears in the
     * message of every constraint error it raises. The V1 migration's {@code RAISE} statements
     * deliberately use the same wording so this one pattern covers them too.
     */
    private static final Pattern CONSTRAINT_IN_MESSAGE =
            Pattern.compile("constraint\\s+\"([^\"]+)\"");

    /**
     * The violated constraint's name, lowercased, if it can be determined.
     *
     * <p>Two strategies, in order, because neither alone is sufficient:
     *
     * <ol>
     *   <li>Hibernate's dialect-driven extraction from
     *       {@link org.hibernate.exception.ConstraintViolationException}. Works for failures raised by
     *       a <em>statement</em> — a unique violation on INSERT, a CHECK on UPDATE.
     *   <li>Parsing the message of any {@link SQLException} in the cause chain. This is the case that
     *       matters for the deferred double-entry trigger: it is
     *       {@code DEFERRABLE INITIALLY DEFERRED}, so it fires at <em>COMMIT</em>, and the exception
     *       Spring produces then is a plain {@code DataIntegrityViolationException} wrapping "Unable
     *       to commit against JDBC Connection" — with no Hibernate {@code ConstraintViolationException}
     *       anywhere in the chain. Strategy 1 returns nothing for it.
     * </ol>
     *
     * <p>Message parsing is normally a smell, and it is used here with open eyes. The alternative is
     * {@code PSQLException.getServerErrorMessage().getConstraint()}, which would mean either promoting
     * the JDBC driver to compile scope or reaching for reflection. PostgreSQL's wording for constraint
     * errors is stable across major versions, the migration's own {@code RAISE} statements are in this
     * repository and match it deliberately, and strategy 1 still handles the common path — so the
     * fallback is narrow rather than load-bearing.
     *
     * <p>Empty for failures with no constraint at all: a serialisation failure, a connection reset, a
     * lock timeout.
     */
    public static Optional<String> constraintName(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current instanceof org.hibernate.exception.ConstraintViolationException hibernate) {
                String name = hibernate.getConstraintName();
                if (name != null && !name.isBlank()) {
                    return Optional.of(name.toLowerCase(Locale.ROOT));
                }
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return constraintNameFromMessage(throwable);
    }

    private static Optional<String> constraintNameFromMessage(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            String message = current.getMessage();
            if (message != null) {
                Matcher matcher = CONSTRAINT_IN_MESSAGE.matcher(message);
                if (matcher.find()) {
                    return Optional.of(matcher.group(1).toLowerCase(Locale.ROOT));
                }
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return Optional.empty();
    }

    /** True when {@code throwable} was caused by a violation of the named constraint. */
    public static boolean isViolationOf(Throwable throwable, String constraintName) {
        return constraintName(throwable)
                .filter(name -> name.equals(constraintName.toLowerCase(Locale.ROOT)))
                .isPresent();
    }

    /** The five-character SQLSTATE of the first {@link SQLException} in the cause chain. */
    public static Optional<String> sqlState(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current instanceof SQLException sqlException) {
                String state = sqlException.getSQLState();
                if (state != null && !state.isBlank()) {
                    return Optional.of(state);
                }
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return Optional.empty();
    }

    public static boolean hasSqlState(Throwable throwable, String expected) {
        return sqlState(throwable).filter(expected::equals).isPresent();
    }

    /** SQLSTATE {@code 23505}. */
    public static boolean isUniqueViolation(Throwable throwable) {
        return hasSqlState(throwable, DatabaseConstraints.SQLSTATE_UNIQUE_VIOLATION);
    }

    /** SQLSTATE {@code 23514}, which the ledger's raising triggers also use. */
    public static boolean isCheckViolation(Throwable throwable) {
        return hasSqlState(throwable, DatabaseConstraints.SQLSTATE_CHECK_VIOLATION);
    }

    /** SQLSTATE {@code 23503}. */
    public static boolean isForeignKeyViolation(Throwable throwable) {
        return hasSqlState(throwable, DatabaseConstraints.SQLSTATE_FOREIGN_KEY_VIOLATION);
    }

    /**
     * SQLSTATE {@code 0A000}, raised only by {@code apex_forbid_mutation()}.
     *
     * <p>Reaching this means something attempted to rewrite ledger history and every earlier layer
     * failed to stop it. Treat as a defect, not a handleable error.
     */
    public static boolean isAppendOnlyViolation(Throwable throwable) {
        return hasSqlState(throwable, DatabaseConstraints.SQLSTATE_APPEND_ONLY_VIOLATION);
    }
}
