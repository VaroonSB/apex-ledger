package com.apex.ledger.infrastructure.persistence.entity;

import com.apex.ledger.domain.model.OutboxStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A domain event staged for publication by the transactional outbox pattern.
 *
 * <p>Written in the <em>same</em> database transaction as the journal entries it describes. That is
 * the entire value of the pattern: an event exists if and only if the ledger change committed. The
 * alternative — publishing to Kafka from inside the transaction — can either publish an event for a
 * transfer that then rolls back, or commit a transfer whose event never reaches the broker. Neither
 * is acceptable when downstream systems derive balances from the stream.
 *
 * <p><strong>This is the one mutable entity in the ledger, and legitimately so.</strong> The fields
 * that change ({@code status}, {@code attempts}, {@code available_at}, {@code published_at},
 * {@code last_error}) are delivery bookkeeping, not accounting history. The immutable part — what
 * happened — lives in {@code payload}, which has no mutator.
 *
 * <p>Delivery is <b>at-least-once</b>, not exactly-once: the relay can publish successfully and then
 * fail before marking the row, so the event is republished. Consumers must deduplicate on
 * {@link #getEventId()}, which is stable across every redelivery. Claiming a database row and
 * acknowledging a Kafka write cannot be made atomic, so this is inherent to the pattern rather than a
 * shortcoming of this implementation.
 *
 * <p>The primary key is a {@code BIGINT} identity rather than a UUID because the relay reads in
 * primary-key order and benefits from dense, insertion-ordered keys. The cost is that Hibernate
 * cannot batch inserts for an {@code IDENTITY} entity — acceptable at one outbox row per ledger
 * transaction. Identity values also have gaps, since a rolled-back transaction still consumes its
 * sequence value; the relay must therefore never treat a missing id as an error.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    /** Stable business identity, carried to Kafka for consumer-side deduplication. */
    @Column(name = "event_id", nullable = false, updatable = false)
    private UUID eventId;

    @Column(name = "aggregate_type", nullable = false, updatable = false, length = 64)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private UUID aggregateId;

    @Column(name = "event_type", nullable = false, updatable = false, length = 128)
    private String eventType;

    @Column(name = "topic", nullable = false, updatable = false, length = 255)
    private String topic;

    /**
     * Kafka record key. Keying by account is what preserves per-account ordering downstream, since
     * ordering is only guaranteed within a partition.
     */
    @Column(name = "partition_key", nullable = false, updatable = false, length = 255)
    private String partitionKey;

    /**
     * Serialised event body, stored as {@code jsonb}. Immutable: the record of what happened is never
     * rewritten, only its delivery state is.
     *
     * <p><strong>Not byte-preserved.</strong> {@code jsonb} parses the document and stores a
     * canonical form, so what comes back out has normalised whitespace, reordered keys and any
     * duplicate key dropped. Two consequences worth knowing:
     *
     * <ul>
     *   <li>Never compute a signature or digest over this column and expect it to match one taken
     *       before the write — sign the payload before it is stored, and carry the signature in a
     *       separate field.
     *   <li>Numeric precision <em>is</em> safe: {@code jsonb} keeps a JSON number as PostgreSQL
     *       {@code numeric}, so a money amount neither loses its scale nor degrades to a double. That
     *       is the property that actually matters here, and it is why {@code jsonb} is acceptable
     *       despite the normalisation.
     * </ul>
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private String payload;

    /** Transport headers, e.g. trace context so a consumer joins the producing trace. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "headers", nullable = false, updatable = false)
    private String headers;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private OutboxStatus status;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    /** Retry gate: the relay only claims rows whose {@code available_at} has passed. */
    @Column(name = "available_at", nullable = false)
    private Instant availableAt;

    /** When the domain event occurred, as distinct from when its row was written. */
    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "last_error")
    private String lastError;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    /** For Hibernate only. */
    protected OutboxEvent() {
        // Intentionally empty.
    }

    private OutboxEvent(UUID eventId, String aggregateType, UUID aggregateId, String eventType,
                        String topic, String partitionKey, String payload, String headers,
                        Instant occurredAt, Instant createdAt) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.aggregateType = requireNotBlank(aggregateType, "aggregateType");
        this.aggregateId = Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        this.eventType = requireNotBlank(eventType, "eventType");
        this.topic = requireNotBlank(topic, "topic");
        this.partitionKey = requireNotBlank(partitionKey, "partitionKey");
        this.payload = requireNotBlank(payload, "payload");
        this.headers = headers == null || headers.isBlank() ? "{}" : headers;
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.status = OutboxStatus.PENDING;
        this.attempts = 0;
        // Immediately claimable.
        this.availableAt = createdAt;
        this.publishedAt = null;
    }

    /**
     * Stages an event for publication.
     *
     * @param payload JSON object as a string; must be an object, per
     *     {@code ck_outbox_events_payload_is_object}
     * @param headers JSON object as a string, or {@code null} for {@code {}}
     */
    public static OutboxEvent pending(String aggregateType, UUID aggregateId, String eventType,
                                      String topic, String partitionKey, String payload,
                                      String headers, Instant occurredAt, Instant createdAt) {
        return new OutboxEvent(UUID.randomUUID(), aggregateType, aggregateId, eventType, topic,
                partitionKey, payload, headers, occurredAt, createdAt);
    }

    private static String requireNotBlank(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    // ---------------------------------------------------------------------
    // Dispatch state transitions
    // ---------------------------------------------------------------------

    /**
     * Records a successful publication.
     *
     * <p>Sets {@code published_at} together with the status, because
     * {@code ck_outbox_events_published_at} requires the two to agree: {@code PUBLISHED} exactly when
     * {@code published_at} is non-null.
     */
    public void markPublished(Instant publishedAt) {
        Objects.requireNonNull(publishedAt, "publishedAt must not be null");
        requireNotTerminal("markPublished");
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = publishedAt;
        this.lastError = null;
        this.attempts++;
    }

    /**
     * Records a failed publication and schedules the next attempt.
     *
     * @param retryAt when the relay may claim this row again — the caller supplies the backoff
     */
    public void markFailed(String error, Instant retryAt) {
        Objects.requireNonNull(retryAt, "retryAt must not be null");
        requireNotTerminal("markFailed");
        this.status = OutboxStatus.FAILED;
        this.attempts++;
        this.availableAt = retryAt;
        this.lastError = truncateError(error);
        // published_at deliberately left null: the CHECK constraint forbids it on a non-PUBLISHED row.
    }

    /**
     * Gives up on this event. Terminal, and requires operator attention: an abandoned event means a
     * downstream consumer will never see a ledger change that did commit.
     */
    public void abandon(String error) {
        requireNotTerminal("abandon");
        this.status = OutboxStatus.ABANDONED;
        this.lastError = truncateError(error);
    }

    private void requireNotTerminal(String operation) {
        if (status != null && status.isTerminal()) {
            throw new IllegalStateException(
                    "cannot %s outbox event %s: already in terminal state %s"
                            .formatted(operation, eventId, status));
        }
    }

    /** {@code last_error} is unbounded TEXT, but a multi-megabyte stack trace helps nobody. */
    private static String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 4000 ? error : error.substring(0, 4000);
    }

    // ---------------------------------------------------------------------
    // Accessors
    // ---------------------------------------------------------------------

    public Long getId() {
        return id;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getPayload() {
        return payload;
    }

    public String getHeaders() {
        return headers;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getAvailableAt() {
        return availableAt;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Optional<Instant> getPublishedAt() {
        return Optional.ofNullable(publishedAt);
    }

    public Optional<String> getLastError() {
        return Optional.ofNullable(lastError);
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OutboxEvent that)) {
            return false;
        }
        // eventId is assigned at construction, so it identifies the instance even before the
        // IDENTITY primary key has been generated.
        return eventId != null && eventId.equals(that.eventId);
    }

    @Override
    public int hashCode() {
        return eventId == null ? 0 : eventId.hashCode();
    }

    @Override
    public String toString() {
        return "OutboxEvent[id=%s, eventId=%s, type=%s, status=%s, attempts=%d]"
                .formatted(id, eventId, eventType, status, attempts);
    }
}
