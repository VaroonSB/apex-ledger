package com.apex.ledger.domain.model;

import java.util.Objects;

/**
 * A client-supplied key identifying one logical submission attempt.
 *
 * <p>The client owns the value; the ledger only guarantees that at most one transaction ever exists
 * per key. That guarantee is the unique constraint {@code uq_transactions_idempotency_key} — not
 * this type, and not the Redis fast path.
 *
 * <p>Bounded at 255 characters to match {@code transactions.idempotency_key}. Surrounding whitespace
 * is rejected rather than trimmed: if a client sends {@code " k"} on the first attempt and {@code "k"}
 * on the retry, silently normalising both to {@code "k"} would be right, but silently accepting them
 * as *different* keys would double-post. Refusing the ambiguous form outright avoids having to guess.
 */
public record IdempotencyKey(String value) implements Comparable<IdempotencyKey> {

    public static final int MAX_LENGTH = 255;

    public IdempotencyKey {
        Objects.requireNonNull(value, "idempotency key must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("idempotency key must not be blank");
        }
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(
                    "idempotency key must be at most %d characters, got %d"
                            .formatted(MAX_LENGTH, value.length()));
        }
        if (!value.equals(value.strip())) {
            throw new IllegalArgumentException(
                    "idempotency key must not have leading or trailing whitespace: '%s'"
                            .formatted(value));
        }
    }

    public static IdempotencyKey of(String value) {
        return new IdempotencyKey(value);
    }

    @Override
    public int compareTo(IdempotencyKey other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
