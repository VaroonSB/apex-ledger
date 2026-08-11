package com.apex.ledger.application.port.in;

import com.apex.ledger.domain.model.Direction;
import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.domain.model.TransactionKind;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * A request to post one balanced transaction.
 *
 * <p>Deliberately expressed as N legs rather than a from/to pair. A two-account transfer is the common
 * case, but a ledger also has to express a fee split, an FX posting through a position account, or a
 * settlement fanning out across many accounts — all of which are one atomic transaction with more than
 * two legs. Modelling the pair specially would mean a second code path for those, and a second code
 * path is a second place for the double-entry invariant to be wrong.
 */
public record PostTransferCommand(
        IdempotencyKey idempotencyKey,
        TransactionKind kind,
        List<Leg> legs,
        String reference,
        String description,
        Instant effectiveAt,
        String createdBy,
        UUID reversesTransactionId
) {

    /** One side of the posting: an amount, a direction, and the account it lands on. */
    public record Leg(UUID accountId, Direction direction, Money amount) {
        public Leg {
            Objects.requireNonNull(accountId, "accountId must not be null");
            Objects.requireNonNull(direction, "direction must not be null");
            Objects.requireNonNull(amount, "amount must not be null");
            if (!amount.isPositive()) {
                throw new IllegalArgumentException(
                        ("leg amount must be strictly positive, got %s; the direction carries the "
                                + "sign").formatted(amount));
            }
        }
    }

    public PostTransferCommand {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        Objects.requireNonNull(kind, "kind must not be null");
        Objects.requireNonNull(legs, "legs must not be null");
        Objects.requireNonNull(effectiveAt, "effectiveAt must not be null");
        Objects.requireNonNull(createdBy, "createdBy must not be null");
        if (createdBy.isBlank()) {
            throw new IllegalArgumentException("createdBy must not be blank");
        }
        if (legs.size() < 2) {
            throw new IllegalArgumentException(
                    "a double-entry transaction needs at least 2 legs, got " + legs.size());
        }
        // Defensive copy: the command is passed across a lock boundary and must not change underneath.
        legs = List.copyOf(legs);

        // Truncate to microseconds, the resolution PostgreSQL TIMESTAMPTZ actually stores.
        //
        // This is a correctness fix, not tidiness. effectiveAt participates in the request
        // fingerprint, and Instant.now() on Java 21 carries nanoseconds. Left untruncated, the value
        // in a command and the value read back from the database differ in their sub-microsecond
        // digits, so a fingerprint recomputed from persisted data never matches the stored one — and
        // any reconciliation or replay-detection built on that comparison silently reports every
        // request as "key reused with a different payload". Normalising once, here at the boundary,
        // makes the fingerprint stable across a persist/reload round trip.
        effectiveAt = effectiveAt.truncatedTo(ChronoUnit.MICROS);

        boolean requiresLink = kind.requiresReversedTransaction();
        if (requiresLink != (reversesTransactionId != null)) {
            throw new IllegalArgumentException(
                    ("%s %s a reversesTransactionId").formatted(
                            kind, requiresLink ? "requires" : "must not carry"));
        }
    }

    /** Distinct accounts touched by this posting — the set that must be locked. */
    public Set<UUID> involvedAccountIds() {
        return legs.stream()
                .map(Leg::accountId)
                .collect(LinkedHashSet::new, Set::add, Set::addAll);
    }

    /**
     * A stable textual form of this request, for fingerprinting.
     *
     * <p>Two properties matter, and both are about avoiding false idempotency conflicts:
     *
     * <ul>
     *   <li><b>Legs are sorted.</b> The same transfer submitted with its legs in a different order is
     *       the same request, and must produce the same fingerprint.
     *   <li><b>Amounts use {@code toPlainString()}.</b> {@code Money} has already normalised scale per
     *       currency, so {@code 10.5} and {@code 10.50} yield identical text for the same currency.
     * </ul>
     *
     * <p>{@code createdBy} is included: the same movement requested by a different actor is a different
     * request for audit purposes. {@code effectiveAt} is included for the same reason — a backdated
     * correction is not a replay of today's posting.
     */
    public String canonicalForm() {
        StringJoiner legJoiner = new StringJoiner("|");
        legs.stream()
                .sorted(Comparator.comparing((Leg leg) -> leg.accountId().toString())
                        .thenComparing(leg -> leg.direction().name())
                        .thenComparing(leg -> leg.amount().amount()))
                .forEach(leg -> legJoiner.add("%s:%s:%s:%s".formatted(
                        leg.accountId(),
                        leg.direction(),
                        leg.amount().amount().toPlainString(),
                        leg.amount().currency().code())));

        return new StringJoiner("\n")
                .add("kind=" + kind)
                .add("effectiveAt=" + effectiveAt)
                .add("createdBy=" + createdBy)
                .add("reference=" + (reference == null ? "" : reference))
                .add("description=" + (description == null ? "" : description))
                .add("reverses=" + (reversesTransactionId == null ? "" : reversesTransactionId))
                .add("legs=" + legJoiner)
                .toString();
    }
}
