package com.apex.ledger.api.graphql.scalar;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * The custom scalars used by the ApexLedger schema.
 *
 * <p>Hand-written rather than taken from {@code graphql-java-extended-scalars}, for one reason: the
 * off-the-shelf {@code BigDecimal} scalar happily coerces a Java {@code Double}. By the time an amount
 * exists as a {@code double} its precision is already gone, and a scalar that accepts it cannot tell
 * {@code 0.1} from {@code 0.1000000000000000055511151231257827}. In a ledger that is not a rounding
 * nuisance, it is an unattributable discrepancy. {@link #decimal()} refuses the type outright.
 */
public final class LedgerScalars {

    private LedgerScalars() {
        throw new AssertionError("no instances");
    }

    /**
     * An exact decimal, used for every monetary amount in the schema.
     *
     * <h2>Input</h2>
     *
     * <p>Accepted: a {@code String} ({@code "100.00"}), a {@code BigDecimal}, a {@code BigInteger},
     * and the integral Java types. From a query literal, both {@code IntValue} and {@code FloatValue}
     * are accepted because graphql-java parses them into {@code BigInteger} / {@code BigDecimal} — the
     * literal text is preserved exactly, so no precision is lost.
     *
     * <p>Rejected: {@code Double} and {@code Float}. This is the important rule. A JSON float in a
     * variables map only arrives as a {@code Double} if something already parsed it as one; this
     * application configures {@code USE_BIG_DECIMAL_FOR_FLOATS} precisely so that does not happen, and
     * rejecting the type here makes a regression in that configuration fail loudly instead of quietly
     * corrupting amounts.
     *
     * <h2>Output</h2>
     *
     * <p>Always a {@code String} in plain notation. Two reasons, both about the client:
     *
     * <ul>
     *   <li>A JSON number would be parsed by a JavaScript client as an IEEE double, so
     *       {@code 9007199254740993} or a 20-digit balance would be silently altered before the
     *       application ever saw it. A string cannot be.
     *   <li>Plain notation, never {@code 1E+3}: scientific notation loses the scale that encodes the
     *       currency's minor unit, so {@code 1000.00} and {@code 1E+3} would be indistinguishable.
     * </ul>
     */
    public static GraphQLScalarType decimal() {
        return GraphQLScalarType.newScalar()
                .name("Decimal")
                .description("An exact decimal. Accepts a string or an exact number, never a float. "
                        + "Always serialised as a plain-notation string.")
                .coercing(new Coercing<BigDecimal, String>() {

                    @Override
                    public String serialize(Object dataFetcherResult, GraphQLContext context,
                                            Locale locale) throws CoercingSerializeException {
                        BigDecimal value = toBigDecimal(dataFetcherResult,
                                CoercingSerializeException::new);
                        return value.toPlainString();
                    }

                    @Override
                    public BigDecimal parseValue(Object input, GraphQLContext context, Locale locale)
                            throws CoercingParseValueException {
                        return toBigDecimal(input, CoercingParseValueException::new);
                    }

                    @Override
                    public BigDecimal parseLiteral(Value<?> input, CoercedVariables variables,
                                                   GraphQLContext context, Locale locale)
                            throws CoercingParseLiteralException {
                        // graphql-java keeps literals exact: IntValue holds a BigInteger and
                        // FloatValue a BigDecimal, both built from the source text.
                        if (input instanceof StringValue stringValue) {
                            return parseExact(stringValue.getValue(),
                                    CoercingParseLiteralException::new);
                        }
                        if (input instanceof IntValue intValue) {
                            return new BigDecimal(intValue.getValue());
                        }
                        if (input instanceof FloatValue floatValue) {
                            return floatValue.getValue();
                        }
                        throw new CoercingParseLiteralException(
                                "Decimal expects a string or numeric literal, got "
                                        + input.getClass().getSimpleName());
                    }
                })
                .build();
    }

    /**
     * An ISO-8601 instant in UTC.
     *
     * <p>Serialised through {@link Instant#toString()}, which emits {@code Z}-suffixed UTC. Never an
     * epoch number: an audit record has to stay readable by a human years later, and an offsetless
     * local time in a ledger is a defect waiting for a deployment to move region.
     */
    public static GraphQLScalarType dateTime() {
        return GraphQLScalarType.newScalar()
                .name("DateTime")
                .description("An ISO-8601 instant in UTC, e.g. 2026-08-11T13:45:12.123456Z.")
                .coercing(new Coercing<Instant, String>() {

                    @Override
                    public String serialize(Object dataFetcherResult, GraphQLContext context,
                                            Locale locale) throws CoercingSerializeException {
                        if (dataFetcherResult instanceof Instant instant) {
                            return instant.toString();
                        }
                        if (dataFetcherResult instanceof CharSequence text) {
                            // Validate rather than pass through, so a malformed value cannot escape.
                            return parseInstant(text.toString(), CoercingSerializeException::new)
                                    .toString();
                        }
                        throw new CoercingSerializeException(
                                "DateTime cannot serialise " + typeNameOf(dataFetcherResult));
                    }

                    @Override
                    public Instant parseValue(Object input, GraphQLContext context, Locale locale)
                            throws CoercingParseValueException {
                        if (input instanceof Instant instant) {
                            return instant;
                        }
                        if (input instanceof CharSequence text) {
                            return parseInstant(text.toString(), CoercingParseValueException::new);
                        }
                        throw new CoercingParseValueException(
                                "DateTime expects an ISO-8601 string, got " + typeNameOf(input));
                    }

                    @Override
                    public Instant parseLiteral(Value<?> input, CoercedVariables variables,
                                                GraphQLContext context, Locale locale)
                            throws CoercingParseLiteralException {
                        if (input instanceof StringValue stringValue) {
                            return parseInstant(stringValue.getValue(),
                                    CoercingParseLiteralException::new);
                        }
                        throw new CoercingParseLiteralException(
                                "DateTime expects a string literal, got "
                                        + input.getClass().getSimpleName());
                    }
                })
                .build();
    }

    // ------------------------------------------------------------------------

    /** Builds the coercion exception appropriate to the direction being coerced. */
    private interface CoercionFailure {
        RuntimeException create(String message);
    }

    private static BigDecimal toBigDecimal(Object input, CoercionFailure failure) {
        if (input instanceof BigDecimal decimal) {
            return decimal;
        }
        if (input instanceof BigInteger integer) {
            return new BigDecimal(integer);
        }
        if (input instanceof Double || input instanceof Float) {
            throw failure.create(
                    ("Decimal refuses %s: a binary floating-point value has already lost the "
                            + "precision a monetary amount depends on. Send the amount as a string, "
                            + "e.g. \"100.00\".").formatted(typeNameOf(input)));
        }
        if (input instanceof Integer || input instanceof Long || input instanceof Short
                || input instanceof Byte) {
            return BigDecimal.valueOf(((Number) input).longValue());
        }
        if (input instanceof CharSequence text) {
            return parseExact(text.toString(), failure);
        }
        throw failure.create("Decimal cannot handle " + typeNameOf(input));
    }

    private static BigDecimal parseExact(String text, CoercionFailure failure) {
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            throw failure.create("Decimal cannot be an empty string");
        }
        try {
            return new BigDecimal(trimmed);
        } catch (NumberFormatException e) {
            throw failure.create("'%s' is not a valid decimal".formatted(text));
        }
    }

    private static Instant parseInstant(String text, CoercionFailure failure) {
        try {
            return Instant.parse(text.trim());
        } catch (DateTimeParseException e) {
            throw failure.create(
                    ("'%s' is not an ISO-8601 instant; expected a UTC form such as "
                            + "2026-08-11T13:45:12Z").formatted(text));
        }
    }

    private static String typeNameOf(Object value) {
        return value == null ? "null" : value.getClass().getName();
    }
}
