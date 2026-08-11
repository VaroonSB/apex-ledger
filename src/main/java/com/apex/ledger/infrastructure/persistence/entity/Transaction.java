package com.apex.ledger.infrastructure.persistence.entity;

import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.domain.model.RequestFingerprint;
import com.apex.ledger.domain.model.TransactionKind;
import com.apex.ledger.infrastructure.persistence.converter.IdempotencyKeyConverter;
import com.apex.ledger.infrastructure.persistence.converter.RequestFingerprintConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.hibernate.annotations.Immutable;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The immutable header of one accepted business operation.
 *
 * <p>Append-only, by the same five-layer construction as {@link JournalEntry}: no mutators, a
 * {@code final} class, {@code @Immutable}, {@code updatable = false} throughout, and the
 * {@code trg_transactions_append_only} / {@code trg_transactions_no_truncate} triggers.
 *
 * <p>There is deliberately no status field. A transaction row exists only if it committed, so there
 * is no {@code PENDING} to advance and no {@code FAILED} to record — a rolled-back attempt leaves no
 * row at all. Undoing a transaction means recording a new {@link TransactionKind#REVERSAL} that names
 * it through {@link #getReversesTransactionId()}. This is what makes an append-only header viable:
 * with a mutable status, "immutable ledger" would be a claim the schema could not keep.
 *
 * <p><strong>The INSERT of this row is the idempotency reservation.</strong> Because
 * {@code idempotency_key} lives here under {@code uq_transactions_idempotency_key}, claiming the key
 * and writing the postings are the same atomic act. A separate reservation table would need its own
 * transaction and could leak a claim when the posting rolled back, blocking legitimate retries; here,
 * a rollback releases the key automatically. The corollary is intentional: a failed attempt does not
 * consume its key, so a client may retry after a genuine failure.
 */
@Entity
@Immutable
@Table(name = "transactions")
public final class Transaction implements Persistable<UUID> {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Convert(converter = IdempotencyKeyConverter.class)
    @Column(name = "idempotency_key", nullable = false, updatable = false, length = 255)
    private IdempotencyKey idempotencyKey;

    @Convert(converter = RequestFingerprintConverter.class)
    @Column(name = "request_fingerprint", nullable = false, updatable = false, length = 64)
    private RequestFingerprint requestFingerprint;

    @Enumerated(EnumType.STRING)
    @Column(name = "kind", nullable = false, updatable = false, length = 16)
    private TransactionKind kind;

    @Column(name = "reference", updatable = false, length = 128)
    private String reference;

    @Column(name = "description", updatable = false, length = 512)
    private String description;

    /** Set if and only if {@link #kind} is {@link TransactionKind#REVERSAL}. */
    @Column(name = "reverses_transaction_id", updatable = false)
    private UUID reversesTransactionId;

    /**
     * Business date this transaction belongs to, which may precede {@link #createdAt} for a backdated
     * correction. Keeping both is what makes a restatement auditable: {@code effective_at} answers
     * "which accounting period" and {@code created_at} answers "when did we learn of it".
     */
    @Column(name = "effective_at", nullable = false, updatable = false)
    private Instant effectiveAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "created_by", nullable = false, updatable = false, length = 128)
    private String createdBy;

    /** See {@link JournalEntry#isNew()} — avoids a SELECT-before-INSERT on every save. */
    @Transient
    private boolean persisted;

    /** For Hibernate only. */
    protected Transaction() {
        // Intentionally empty.
    }

    private Transaction(UUID id, IdempotencyKey idempotencyKey,
                        RequestFingerprint requestFingerprint, TransactionKind kind,
                        String reference, String description, UUID reversesTransactionId,
                        Instant effectiveAt, Instant createdAt, String createdBy) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
        this.requestFingerprint =
                Objects.requireNonNull(requestFingerprint, "requestFingerprint must not be null");
        this.kind = Objects.requireNonNull(kind, "kind must not be null");
        this.effectiveAt = Objects.requireNonNull(effectiveAt, "effectiveAt must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.createdBy = requireNotBlank(createdBy, "createdBy");
        this.reference = reference;
        this.description = description;
        this.reversesTransactionId = reversesTransactionId;

        // Mirrors ck_transactions_reversal_link. Checked here too so the failure names the field
        // rather than surfacing as an opaque constraint violation at flush time.
        boolean hasLink = reversesTransactionId != null;
        if (kind.requiresReversedTransaction() != hasLink) {
            throw new IllegalArgumentException(
                    ("%s %s a reversed-transaction reference, but %s was supplied")
                            .formatted(kind,
                                    kind.requiresReversedTransaction() ? "requires" : "must not have",
                                    hasLink ? "one" : "none"));
        }
        if (hasLink && reversesTransactionId.equals(id)) {
            throw new IllegalArgumentException(
                    "transaction %s cannot reverse itself".formatted(id));
        }
    }

    /** Creates a {@link TransactionKind#TRANSFER} or {@link TransactionKind#ADJUSTMENT}. */
    public static Transaction of(IdempotencyKey idempotencyKey,
                                 RequestFingerprint requestFingerprint, TransactionKind kind,
                                 String reference, String description, Instant effectiveAt,
                                 Instant createdAt, String createdBy) {
        if (kind.requiresReversedTransaction()) {
            throw new IllegalArgumentException(
                    "%s must be created with reversalOf(...)".formatted(kind));
        }
        return new Transaction(UUID.randomUUID(), idempotencyKey, requestFingerprint, kind,
                reference, description, null, effectiveAt, createdAt, createdBy);
    }

    /**
     * Creates a {@link TransactionKind#REVERSAL} offsetting {@code reversedTransactionId}.
     *
     * <p>At most one reversal may exist per original transaction; a second attempt violates
     * {@code uq_transactions_reverses}.
     */
    public static Transaction reversalOf(UUID reversedTransactionId, IdempotencyKey idempotencyKey,
                                         RequestFingerprint requestFingerprint, String reference,
                                         String description, Instant effectiveAt, Instant createdAt,
                                         String createdBy) {
        Objects.requireNonNull(reversedTransactionId, "reversedTransactionId must not be null");
        return new Transaction(UUID.randomUUID(), idempotencyKey, requestFingerprint,
                TransactionKind.REVERSAL, reference, description, reversedTransactionId,
                effectiveAt, createdAt, createdBy);
    }

    private static String requireNotBlank(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    @PostPersist
    @PostLoad
    void markPersisted() {
        this.persisted = true;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return !persisted;
    }

    public IdempotencyKey getIdempotencyKey() {
        return idempotencyKey;
    }

    public RequestFingerprint getRequestFingerprint() {
        return requestFingerprint;
    }

    public TransactionKind getKind() {
        return kind;
    }

    public Optional<String> getReference() {
        return Optional.ofNullable(reference);
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    public Optional<UUID> getReversesTransactionId() {
        return Optional.ofNullable(reversesTransactionId);
    }

    public boolean isReversal() {
        return kind == TransactionKind.REVERSAL;
    }

    public Instant getEffectiveAt() {
        return effectiveAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    /**
     * Whether {@code candidate} is the same request that produced this transaction. Used to tell a
     * benign retry from key reuse.
     */
    public boolean matchesFingerprint(RequestFingerprint candidate) {
        return requestFingerprint != null && requestFingerprint.matches(candidate);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Transaction that)) {
            return false;
        }
        return id != null && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id == null ? 0 : id.hashCode();
    }

    @Override
    public String toString() {
        return "Transaction[id=%s, kind=%s, idempotencyKey=%s, effectiveAt=%s]"
                .formatted(id, kind, idempotencyKey, effectiveAt);
    }
}
