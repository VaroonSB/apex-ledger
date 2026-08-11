package com.apex.ledger.infrastructure.persistence.entity;

import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.Direction;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.infrastructure.persistence.converter.CurrencyCodeConverter;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A single double-entry posting: one side of a transaction, against one account.
 *
 * <p><strong>This class is the immutability guarantee of the system.</strong> Journal entries are the
 * audit truth; every balance anywhere else is a projection of this table. An entry is therefore
 * written once and never altered. A correction is a new compensating transaction.
 *
 * <p>Immutability is enforced at five independent layers, deliberately redundant because any single
 * one can be bypassed:
 *
 * <ol>
 *   <li><b>No mutators.</b> There is no setter, no builder that mutates a persisted instance, and no
 *       package-private back door. State can only be supplied through the factory methods.
 *   <li><b>{@code final} class.</b> No subclass can add mutable state or override an accessor. Safe
 *       here because nothing holds a lazy {@code @ManyToOne} to a journal entry, so Hibernate never
 *       needs to proxy this type.
 *   <li><b>{@code @Immutable}.</b> Hibernate skips dirty checking and never emits an UPDATE for this
 *       entity. Worth knowing precisely: Hibernate <em>silently ignores</em> a modification rather
 *       than failing, which is exactly why layers 1 and 5 exist rather than relying on this alone.
 *       The useful side effect is performance — no snapshot is retained for a table that will hold
 *       the most rows in the schema.
 *   <li><b>{@code updatable = false}</b> on every column, so even a mapping change or a stray
 *       {@code merge()} cannot produce an UPDATE for these fields.
 *   <li><b>Database triggers.</b> {@code trg_journal_entries_append_only} and
 *       {@code trg_journal_entries_no_truncate} raise on any UPDATE, DELETE or TRUNCATE, including a
 *       zero-row UPDATE, and including statements issued outside this application entirely. This is
 *       the only layer that holds against a human with a psql session.
 * </ol>
 *
 * <p>Note what is <em>not</em> used: {@code private final} fields. Hibernate populates entities
 * reflectively, and while {@code Field.set} does succeed on a non-static final field, the JVM is
 * permitted to treat final field reads as constants and fold them — so a reflectively-assigned final
 * field can be read back as its constructor-time value. Non-final fields with no mutators give the
 * same guarantee to callers without that hazard.
 *
 * <p>Associations to {@code Transaction} and {@code Account} are held as raw {@link UUID} keys rather
 * than as mapped entities. On the highest-volume table in the schema this avoids proxy initialisation
 * and accidental cascade behaviour, and it keeps the entry a leaf with no object graph to traverse.
 * Navigation is provided by the repositories, which is where the aggregate boundary belongs.
 */
@Entity
@Immutable
@Table(name = "journal_entries")
public final class JournalEntry implements Persistable<UUID> {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "transaction_id", nullable = false, updatable = false)
    private UUID transactionId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    /**
     * Position within the owning transaction. {@code SMALLINT}, hence {@code short}: mapping this as
     * {@code int} would make Hibernate expect an {@code INTEGER} column and fail schema validation.
     */
    @Column(name = "entry_sequence", nullable = false, updatable = false)
    private short entrySequence;

    @Enumerated(EnumType.STRING)
    @Column(name = "direction", nullable = false, updatable = false, length = 6)
    private Direction direction;

    /**
     * Always strictly positive; the sign is carried by {@link #direction}. Guarded by
     * {@code ck_journal_entries_amount_positive} as well as the factory below.
     */
    @Column(name = "amount", nullable = false, updatable = false, precision = 38, scale = 18)
    private BigDecimal amount;

    /**
     * Denormalised from the account. The composite foreign key
     * {@code fk_journal_entries_account_currency} proves this always equals the account's currency,
     * so no application check is needed.
     */
    @Convert(converter = CurrencyCodeConverter.class)
    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private CurrencyCode currency;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /**
     * Drives {@link #isNew()}. Without it, Spring Data would see a non-null assigned id, treat every
     * instance as detached and route {@code save()} through {@code merge()} — costing a SELECT before
     * each INSERT on the busiest table in the system.
     */
    @Transient
    private boolean persisted;

    /** For Hibernate only. */
    protected JournalEntry() {
        // Intentionally empty: Hibernate populates fields reflectively.
    }

    private JournalEntry(UUID id, UUID transactionId, UUID accountId, short entrySequence,
                         Direction direction, Money money, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.transactionId = Objects.requireNonNull(transactionId, "transactionId must not be null");
        this.accountId = Objects.requireNonNull(accountId, "accountId must not be null");
        this.entrySequence = entrySequence;
        this.direction = Objects.requireNonNull(direction, "direction must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");

        Objects.requireNonNull(money, "money must not be null");
        if (!money.isPositive()) {
            throw new IllegalArgumentException(
                    ("journal entry amount must be strictly positive, got %s; the debit/credit "
                            + "direction carries the sign, never the amount")
                            .formatted(money));
        }
        this.amount = money.amount();
        this.currency = money.currency();
    }

    /**
     * Creates a posting.
     *
     * @param entrySequence position within the transaction, {@code 0}-based
     * @throws IllegalArgumentException if the amount is not strictly positive, or the sequence does
     *     not fit the {@code SMALLINT} column
     */
    public static JournalEntry of(UUID transactionId, UUID accountId, int entrySequence,
                                  Direction direction, Money money, Instant createdAt) {
        if (entrySequence < 0 || entrySequence > Short.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "entrySequence must be between 0 and %d, got %d"
                            .formatted(Short.MAX_VALUE, entrySequence));
        }
        return new JournalEntry(UUID.randomUUID(), transactionId, accountId, (short) entrySequence,
                direction, money, createdAt);
    }

    public static JournalEntry debit(UUID transactionId, UUID accountId, int entrySequence,
                                     Money money, Instant createdAt) {
        return of(transactionId, accountId, entrySequence, Direction.DEBIT, money, createdAt);
    }

    public static JournalEntry credit(UUID transactionId, UUID accountId, int entrySequence,
                                      Money money, Instant createdAt) {
        return of(transactionId, accountId, entrySequence, Direction.CREDIT, money, createdAt);
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

    public UUID getTransactionId() {
        return transactionId;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public short getEntrySequence() {
        return entrySequence;
    }

    public Direction getDirection() {
        return direction;
    }

    /** The posting amount, always positive. */
    public Money getAmount() {
        return Money.of(amount, currency);
    }

    /**
     * The amount with {@link Direction#signum()} applied: positive for a debit, negative for a
     * credit. Summing this over a transaction must yield zero per currency, which is precisely what
     * {@code apex_assert_transaction_balanced()} verifies at COMMIT.
     */
    public Money getSignedAmount() {
        Money positive = getAmount();
        return direction == Direction.DEBIT ? positive : positive.negated();
    }

    public CurrencyCode getCurrency() {
        return currency;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    /** Entity identity: the assigned primary key, which never changes. */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof JournalEntry that)) {
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
        return "JournalEntry[id=%s, txn=%s, account=%s, seq=%d, %s %s]"
                .formatted(id, transactionId, accountId, entrySequence, direction,
                        amount == null ? "?" : amount.toPlainString() + " " + currency);
    }
}
