package com.apex.ledger.domain.model;

import java.util.Currency;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * A validated ISO-4217 currency code.
 *
 * <p>Exists so a currency can never be an arbitrary {@code String}. Construction rejects anything
 * that is not a real, transactable ISO-4217 code, which means every {@link Money} in the system
 * carries a currency whose minor-unit count is known.
 *
 * <p>Pseudo-currencies are rejected. {@link Currency#getDefaultFractionDigits()} returns {@code -1}
 * for entries like {@code XAU} (gold) and {@code XDR} (IMF special drawing rights); those are units
 * of account, not something a ledger can post a scaled amount in, so they are refused rather than
 * silently treated as zero-decimal.
 *
 * <p>Crypto assets are deliberately out of scope: they are not in ISO-4217 and would need their own
 * registry with per-asset precision (18 fraction digits for wei-denominated tokens, for instance).
 * Adding them means replacing the {@link Currency} lookup here, not widening the pattern.
 */
public record CurrencyCode(String code) implements Comparable<CurrencyCode> {

    private static final Pattern ISO_4217_FORMAT = Pattern.compile("^[A-Z]{3}$");

    /**
     * Minor-unit cache. Read without locking on the hot path: {@code Currency.getInstance} is not
     * free, and this value is consulted on every {@code Money} construction. A rare duplicate
     * computation under contention is cheaper than the bin lock {@code computeIfAbsent} would take,
     * which on Java 21 would pin a carrier thread if it ever blocked.
     */
    private static final Map<String, Integer> MINOR_UNITS_BY_CODE = new ConcurrentHashMap<>();

    public CurrencyCode {
        Objects.requireNonNull(code, "currency code must not be null");
        if (!ISO_4217_FORMAT.matcher(code).matches()) {
            throw new IllegalArgumentException(
                    "currency code must be three uppercase letters, got '%s'".formatted(code));
        }
        // Validates at construction so an invalid code cannot exist as a CurrencyCode instance.
        resolveMinorUnits(code);
    }

    public static CurrencyCode of(String code) {
        return new CurrencyCode(code);
    }

    /**
     * Number of fraction digits this currency permits: 2 for USD, 0 for JPY, 3 for KWD.
     *
     * <p>This is the authority for how precise an amount in this currency may be. {@link Money}
     * refuses anything finer.
     */
    public int minorUnits() {
        return resolveMinorUnits(code);
    }

    private static int resolveMinorUnits(String code) {
        Integer cached = MINOR_UNITS_BY_CODE.get(code);
        if (cached != null) {
            return cached;
        }
        Currency currency;
        try {
            currency = Currency.getInstance(code);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "'%s' is not a known ISO-4217 currency code".formatted(code), e);
        }
        int digits = currency.getDefaultFractionDigits();
        if (digits < 0) {
            throw new IllegalArgumentException(
                    ("'%s' is a unit of account, not a transactable currency "
                            + "(no defined minor unit); it cannot be used in a ledger posting")
                            .formatted(code));
        }
        MINOR_UNITS_BY_CODE.putIfAbsent(code, digits);
        return digits;
    }

    @Override
    public int compareTo(CurrencyCode other) {
        return code.compareTo(other.code);
    }

    @Override
    public String toString() {
        return code;
    }
}
