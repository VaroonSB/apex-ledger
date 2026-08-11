package com.apex.ledger.api.graphql.support;

import com.apex.ledger.api.graphql.dto.JournalEntryView;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

/**
 * Opaque keyset cursor naming a position in an account's statement.
 *
 * <p>Encodes the {@code (createdAt, id)} of an entry — the exact tuple
 * {@code idx_journal_entries_account_created} is ordered by — so resuming a page is an index seek
 * rather than an offset scan. On an append-only journal that difference is not just performance: an
 * offset shifts every time a posting lands ahead of the window, so an offset-paged client silently
 * skips or repeats entries while it reads. A keyset cursor names a fixed point in a total order and is
 * unaffected by concurrent inserts.
 *
 * <p>Both components are required. {@code created_at} alone is not unique — the postings of one
 * transaction share a timestamp — and a cursor that cannot break that tie has no single position to
 * resume from.
 *
 * <p>Base64url with no padding, so the value is URL- and JSON-safe. The encoding is <em>obfuscation,
 * not protection</em>: anyone can decode it. It is opaque so clients do not build cursors by hand and
 * couple themselves to a sort key we may change; it is not a security boundary, and the server
 * re-validates the account id on every request rather than trusting anything the cursor carries.
 */
public record EntryCursor(Instant createdAt, UUID id) {

    private static final String SEPARATOR = "|";
    private static final String PREFIX = "apexje1:";

    public EntryCursor {
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(id, "id must not be null");
    }

    public static EntryCursor of(JournalEntryView entry) {
        return new EntryCursor(entry.createdAt(), entry.id());
    }

    /** The opaque token to hand a client. */
    public String encode() {
        String raw = PREFIX + createdAt + SEPARATOR + id;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Decodes a cursor produced by {@link #encode()}.
     *
     * @throws InvalidCursorException if the token is not a cursor this server issued. Every failure
     *     mode collapses into one exception type with one message: reporting <em>how</em> a cursor is
     *     malformed tells a caller poking at the format exactly how to construct one, and the only
     *     valid response is the same either way — start from the first page.
     */
    public static EntryCursor decode(String token) {
        Objects.requireNonNull(token, "token must not be null");
        String decoded;
        try {
            decoded = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new InvalidCursorException(token, e);
        }
        if (!decoded.startsWith(PREFIX)) {
            throw new InvalidCursorException(token, null);
        }
        String body = decoded.substring(PREFIX.length());
        int separator = body.lastIndexOf(SEPARATOR);
        if (separator < 0) {
            throw new InvalidCursorException(token, null);
        }
        try {
            Instant createdAt = Instant.parse(body.substring(0, separator));
            UUID id = UUID.fromString(body.substring(separator + SEPARATOR.length()));
            return new EntryCursor(createdAt, id);
        } catch (DateTimeParseException | IllegalArgumentException e) {
            throw new InvalidCursorException(token, e);
        }
    }

    /** Raised for any unusable cursor. Mapped to a BAD_REQUEST GraphQL error. */
    public static final class InvalidCursorException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        InvalidCursorException(String token, Throwable cause) {
            super(("'%s' is not a valid cursor. Pass back a value from a previous page's "
                    + "pageInfo.endCursor, or omit `after` to start from the beginning.")
                    .formatted(token), cause);
        }
    }
}
