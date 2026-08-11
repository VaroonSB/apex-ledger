package com.apex.ledger.domain.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * SHA-256 digest of the canonical form of a submitted request.
 *
 * <p>This is what separates an honest retry from a client bug. An idempotency key alone cannot tell
 * the two apart:
 *
 * <ul>
 *   <li>same key, <em>same</em> fingerprint — the client resent the identical request, e.g. after a
 *       socket timeout. Safe to treat as a replay and return the original outcome.
 *   <li>same key, <em>different</em> fingerprint — the client reused a key for different content.
 *       Returning the first transaction's result here would be actively wrong: the caller would be
 *       told a transfer succeeded that the ledger never performed.
 * </ul>
 *
 * <p>Stored as 64 lowercase hex characters, matching {@code ck_transactions_fingerprint_format}.
 *
 * <p>Not a security control. It detects client mistakes, not tampering; anyone who can choose the
 * request body can choose its digest.
 */
public record RequestFingerprint(String value) implements Comparable<RequestFingerprint> {

    public static final int HEX_LENGTH = 64;

    private static final String ALGORITHM = "SHA-256";
    private static final Pattern LOWER_HEX_256 = Pattern.compile("^[0-9a-f]{64}$");

    public RequestFingerprint {
        Objects.requireNonNull(value, "fingerprint must not be null");
        if (!LOWER_HEX_256.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    ("fingerprint must be %d lowercase hex characters (SHA-256), got '%s'")
                            .formatted(HEX_LENGTH, value));
        }
    }

    /** Wraps an already-computed digest, e.g. when reading one back out of the database. */
    public static RequestFingerprint ofHex(String hex) {
        return new RequestFingerprint(hex);
    }

    /**
     * Digests the canonical representation of a request.
     *
     * <p>The caller is responsible for canonicalisation, and it matters: two JSON bodies that differ
     * only in key order or whitespace are the same request but produce different digests, which
     * would surface as a spurious conflict. Serialise from a stable, sorted representation.
     */
    public static RequestFingerprint of(String canonicalRequest) {
        Objects.requireNonNull(canonicalRequest, "canonicalRequest must not be null");
        return new RequestFingerprint(
                HexFormat.of().formatHex(digest(canonicalRequest.getBytes(StandardCharsets.UTF_8))));
    }

    private static byte[] digest(byte[] input) {
        try {
            // Not cached or shared: MessageDigest is stateful and not thread-safe, and under virtual
            // threads a shared instance would be contended by an unbounded number of callers.
            return MessageDigest.getInstance(ALGORITHM).digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    ALGORITHM + " is required by every Java SE implementation but is unavailable", e);
        }
    }

    /** Constant-time comparison, so this stays safe if fingerprints ever gate a decision. */
    public boolean matches(RequestFingerprint other) {
        if (other == null) {
            return false;
        }
        return MessageDigest.isEqual(
                value.getBytes(StandardCharsets.US_ASCII),
                other.value.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public int compareTo(RequestFingerprint other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
