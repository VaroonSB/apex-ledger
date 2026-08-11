package com.apex.ledger.infrastructure.persistence.entity;

import com.apex.ledger.domain.model.AccountStatus;
import com.apex.ledger.domain.model.AccountType;
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
import jakarta.persistence.Version;
import org.hibernate.annotations.Generated;
import org.hibernate.generator.EventType;
import org.springframework.data.domain.Persistable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * An account in the chart of accounts.
 *
 * <p>The only entity in the schema with mutable rows, and its mutability is split in two:
 *
 * <ul>
 *   <li><b>Application-owned:</b> {@code name}, {@code status}, {@code minimum_balance}. Changed
 *       through the intent-named methods below, guarded by the {@code @Version} optimistic lock.
 *   <li><b>Database-owned:</b> {@code total_debits}, {@code total_credits}, {@code balance},
 *       {@code updated_at}. The application <em>never</em> writes these. They are maintained by
 *       {@code trg_journal_entries_apply_balance}, which folds each inserted journal entry into its
 *       account inside the same transaction.
 * </ul>
 *
 * <p>That division is the point. If the application computed balances, a bug could make the
 * projection disagree with the journal and nothing would notice until a reconciliation run. Because
 * the trigger updates the projection in the same statement that writes the entry, the two cannot
 * diverge. It also yields overdraft protection for free: the trigger's UPDATE takes a row lock, so
 * concurrent withdrawals against one account serialise, and each is re-checked against
 * {@code ck_accounts_minimum_balance} using the balance its predecessor left behind.
 *
 * <p><strong>Staleness warning.</strong> {@link #getBalance()} and the totals reflect the moment this
 * instance was loaded or last flushed. Posting journal entries changes them in the database <em>without
 * Hibernate's knowledge</em>, because the trigger — not the ORM — issues that UPDATE. The optimistic
 * {@code version} column is not bumped by the trigger either, so it cannot be used to detect balance
 * drift. Any decision that depends on a current balance must read it under a lock; see
 * {@code AccountRepository#findByIdForUpdate} and {@code #findCurrentBalance}.
 */
@Entity
@Table(name = "accounts")
public class Account implements Persistable<UUID> {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Stable external identifier. Immutable once assigned. */
    @Column(name = "account_number", nullable = false, updatable = false, length = 64)
    private String accountNumber;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    /**
     * Immutable: it determines which side the balance accumulates on, so changing it would silently
     * reinterpret every historical entry.
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "account_type", nullable = false, updatable = false, length = 16)
    private AccountType accountType;

    /**
     * Immutable, and enforced as such by the database: journal entries hold a composite foreign key
     * to {@code (id, currency)}, so PostgreSQL rejects any change to this column on an account that
     * has ever been posted to.
     */
    @Convert(converter = CurrencyCodeConverter.class)
    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private CurrencyCode currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private AccountStatus status;

    /** Database-owned. Lifetime sum of debit postings; monotonically increasing. */
    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "total_debits", precision = 38, scale = 18)
    private BigDecimal totalDebits;

    /** Database-owned. Lifetime sum of credit postings; monotonically increasing. */
    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "total_credits", precision = 38, scale = 18)
    private BigDecimal totalCredits;

    /**
     * Database-owned generated column: signed balance on the account type's natural side. Read from
     * the database rather than recomputed in Java so there is exactly one definition of "balance".
     */
    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "balance", precision = 38, scale = 18)
    private BigDecimal balance;

    /** {@code null} means no floor. Enforced by {@code ck_accounts_minimum_balance}. */
    @Column(name = "minimum_balance", precision = 38, scale = 18)
    private BigDecimal minimumBalance;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** Database-owned, set by {@code trg_accounts_touch_updated_at} on every UPDATE. */
    @Generated(event = {EventType.INSERT, EventType.UPDATE})
    @Column(name = "updated_at")
    private Instant updatedAt;

    @Transient
    private boolean persisted;

    /** For Hibernate only. */
    protected Account() {
        // Intentionally empty.
    }

    private Account(UUID id, String accountNumber, String name, AccountType accountType,
                    CurrencyCode currency, Money minimumBalance, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.accountNumber = requireNotBlank(accountNumber, "accountNumber");
        this.name = requireNotBlank(name, "name");
        this.accountType = Objects.requireNonNull(accountType, "accountType must not be null");
        this.currency = Objects.requireNonNull(currency, "currency must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.status = AccountStatus.ACTIVE;
        this.minimumBalance = toFloor(minimumBalance, currency);
    }

    /**
     * Opens a new {@link AccountStatus#ACTIVE} account with zero balance.
     *
     * @param minimumBalance hard floor, or {@code null} for none. Must be in {@code currency}.
     */
    public static Account open(String accountNumber, String name, AccountType accountType,
                               CurrencyCode currency, Money minimumBalance, Instant createdAt) {
        return new Account(UUID.randomUUID(), accountNumber, name, accountType, currency,
                minimumBalance, createdAt);
    }

    private static BigDecimal toFloor(Money floor, CurrencyCode currency) {
        if (floor == null) {
            return null;
        }
        // Guard here rather than let the mismatch reach the database, where it would appear as an
        // unrelated arithmetic result rather than a currency error.
        Money.zero(currency).requireSameCurrency(floor);
        return floor.amount();
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

    // ---------------------------------------------------------------------
    // Application-owned mutation. Named for intent; there are no generic setters,
    // so every state change at a call site says what it means.
    // ---------------------------------------------------------------------

    public void rename(String newName) {
        this.name = requireNotBlank(newName, "newName");
    }

    /** Blocks posting, reversibly. */
    public void freeze() {
        requireStatus(AccountStatus.ACTIVE, "freeze");
        this.status = AccountStatus.FROZEN;
    }

    public void unfreeze() {
        requireStatus(AccountStatus.FROZEN, "unfreeze");
        this.status = AccountStatus.ACTIVE;
    }

    /**
     * Retires the account permanently.
     *
     * <p>Only checks that the transition is legal. It deliberately does <em>not</em> assert a zero
     * balance from {@link #getBalance()}: that value may be stale (see the class comment), and
     * refusing or permitting a close on stale data would be worse than not checking. The
     * zero-balance precondition belongs in the service layer, where the row is locked.
     */
    public void close() {
        if (status.isTerminal()) {
            throw new IllegalStateException(
                    "account %s is already CLOSED".formatted(accountNumber));
        }
        this.status = AccountStatus.CLOSED;
    }

    /**
     * Changes the overdraft floor.
     *
     * <p>If the new floor is above the current balance the database rejects the UPDATE via
     * {@code ck_accounts_minimum_balance}, which is the intended behaviour: a floor that the account
     * already violates must not be recorded.
     *
     * @param newFloor the new floor, or {@code null} to remove it
     */
    public void updateMinimumBalance(Money newFloor) {
        this.minimumBalance = toFloor(newFloor, currency);
    }

    private void requireStatus(AccountStatus expected, String operation) {
        if (status != expected) {
            throw new IllegalStateException(
                    "cannot %s account %s: status is %s, expected %s"
                            .formatted(operation, accountNumber, status, expected));
        }
    }

    // ---------------------------------------------------------------------
    // Accessors
    // ---------------------------------------------------------------------

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public boolean isNew() {
        return !persisted;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public String getName() {
        return name;
    }

    public AccountType getAccountType() {
        return accountType;
    }

    public CurrencyCode getCurrency() {
        return currency;
    }

    public AccountStatus getStatus() {
        return status;
    }

    /** The side on which this account's balance accumulates. */
    public Direction getNormalBalance() {
        return accountType.normalBalance();
    }

    public boolean canPost() {
        return status.postingAllowed();
    }

    /**
     * Signed balance as of load or last flush.
     *
     * <p>May be stale — journal postings update it by trigger, invisibly to Hibernate. Read it under
     * a lock before acting on it.
     */
    public Money getBalance() {
        return Money.of(balance == null ? BigDecimal.ZERO : balance, currency);
    }

    /** Lifetime debit total as of load or last flush. See {@link #getBalance()} on staleness. */
    public Money getTotalDebits() {
        return Money.of(totalDebits == null ? BigDecimal.ZERO : totalDebits, currency);
    }

    /** Lifetime credit total as of load or last flush. See {@link #getBalance()} on staleness. */
    public Money getTotalCredits() {
        return Money.of(totalCredits == null ? BigDecimal.ZERO : totalCredits, currency);
    }

    public Optional<Money> getMinimumBalance() {
        return Optional.ofNullable(minimumBalance).map(value -> Money.of(value, currency));
    }

    public long getVersion() {
        return version;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Account that)) {
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
        return "Account[id=%s, number=%s, type=%s, currency=%s, status=%s]"
                .formatted(id, accountNumber, accountType, currency, status);
    }
}
