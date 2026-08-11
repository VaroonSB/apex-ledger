package com.apex.ledger.domain.model;

import com.apex.ledger.domain.exception.CurrencyMismatchException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * An immutable monetary amount in a single currency.
 *
 * <p><strong>Canonical scale.</strong> The amount is always rescaled at construction to exactly the
 * currency's minor-unit count. Two consequences, both intentional:
 *
 * <ul>
 *   <li>An amount finer than the currency permits is <em>rejected</em>, not rounded. {@code 1.005
 *       USD} throws. Silently rounding sub-cent input is how a ledger acquires a rounding-error
 *       drift that nobody can later attribute.
 *   <li>Because scale is canonical per currency, {@code equals} is well behaved. Raw
 *       {@link BigDecimal#equals} is scale-sensitive — {@code 1.0} is not equal to {@code 1.00} —
 *       which makes unnormalised BigDecimal a hazardous map key or assertion target. After
 *       normalisation, {@code Money.of("1.0", USD)} and {@code Money.of("1.00", USD)} are equal.
 * </ul>
 *
 * <p>Reading back from PostgreSQL is safe: a {@code NUMERIC(38,18)} column returns scale 18, and
 * rescaling to 2 for USD only strips trailing zeros. If a stored value genuinely carries more
 * precision than its currency allows, that is data corruption and construction throws rather than
 * quietly truncating it.
 *
 * <p>Bounds match the {@code NUMERIC(38,18)} storage type, so a {@code Money} that constructs
 * successfully can always be persisted without overflow.
 */
public record Money(BigDecimal amount, CurrencyCode currency) implements Comparable<Money> {

    /** Total significant digits available in the {@code NUMERIC(38,18)} storage columns. */
    public static final int MAX_PRECISION = 38;

    /** Fraction digits available in storage. Far beyond any ISO-4217 currency. */
    public static final int MAX_SCALE = 18;

    /** Digits available left of the decimal point: 38 - 18 = 20. */
    public static final int MAX_INTEGER_DIGITS = MAX_PRECISION - MAX_SCALE;

    public Money {
        Objects.requireNonNull(currency, "currency must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
        amount = canonicalise(amount, currency);
    }

    public static Money of(BigDecimal amount, CurrencyCode currency) {
        return new Money(amount, currency);
    }

    public static Money of(String amount, String currency) {
        return new Money(new BigDecimal(amount), CurrencyCode.of(currency));
    }

    public static Money of(String amount, CurrencyCode currency) {
        return new Money(new BigDecimal(amount), currency);
    }

    public static Money zero(CurrencyCode currency) {
        return new Money(BigDecimal.ZERO, currency);
    }

    private static BigDecimal canonicalise(BigDecimal raw, CurrencyCode currency) {
        int minorUnits = currency.minorUnits();
        BigDecimal scaled;
        try {
            scaled = raw.setScale(minorUnits, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    ("amount %s is more precise than %s permits: %s allows %d fraction digit(s), "
                            + "and rounding money implicitly is not allowed")
                            .formatted(raw.toPlainString(), currency, currency, minorUnits), e);
        }
        // precision() - scale() is the count of integer digits; BigDecimal reports precision 1 for
        // zero, which can make the difference non-positive, hence the floor at 1.
        int integerDigits = Math.max(1, scaled.precision() - scaled.scale());
        if (integerDigits > MAX_INTEGER_DIGITS) {
            throw new IllegalArgumentException(
                    ("amount %s has %d integer digits, exceeding the %d supported by "
                            + "NUMERIC(%d,%d) storage")
                            .formatted(scaled.toPlainString(), integerDigits, MAX_INTEGER_DIGITS,
                                    MAX_PRECISION, MAX_SCALE));
        }
        return scaled;
    }

    public Money plus(Money other) {
        requireSameCurrency(other);
        return new Money(amount.add(other.amount), currency);
    }

    public Money minus(Money other) {
        requireSameCurrency(other);
        return new Money(amount.subtract(other.amount), currency);
    }

    public Money negated() {
        return new Money(amount.negate(), currency);
    }

    public Money abs() {
        return isNegative() ? negated() : this;
    }

    public boolean isZero() {
        return amount.signum() == 0;
    }

    public boolean isPositive() {
        return amount.signum() > 0;
    }

    public boolean isNegative() {
        return amount.signum() < 0;
    }

    public int signum() {
        return amount.signum();
    }

    public boolean isSameCurrencyAs(Money other) {
        return currency.equals(other.currency);
    }

    /**
     * @throws CurrencyMismatchException if the currencies differ. A ledger never performs implicit
     *     conversion: crossing currencies is an explicit FX posting through a position account, not
     *     an arithmetic side effect.
     */
    public void requireSameCurrency(Money other) {
        Objects.requireNonNull(other, "other must not be null");
        if (!isSameCurrencyAs(other)) {
            throw new CurrencyMismatchException(currency, other.currency);
        }
    }

    /** Never scientific notation — {@code 1E+3} in an audit record is unacceptable. */
    public String toPlainString() {
        return amount.toPlainString();
    }

    @Override
    public int compareTo(Money other) {
        requireSameCurrency(other);
        return amount.compareTo(other.amount);
    }

    @Override
    public String toString() {
        return amount.toPlainString() + " " + currency;
    }
}
