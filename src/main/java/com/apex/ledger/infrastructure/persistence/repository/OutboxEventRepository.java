package com.apex.ledger.infrastructure.persistence.repository;

import com.apex.ledger.domain.model.OutboxStatus;
import com.apex.ledger.infrastructure.persistence.entity.OutboxEvent;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence access for the transactional outbox.
 *
 * <p>Unlike the ledger repositories this one does expose deletion, because pruning published history
 * is a routine operation — an outbox row is a delivery receipt, not accounting history.
 */
public interface OutboxEventRepository extends Repository<OutboxEvent, Long> {

    OutboxEvent save(OutboxEvent event);

    Optional<OutboxEvent> findById(Long id);

    Optional<OutboxEvent> findByEventId(UUID eventId);

    /**
     * Events in a given dispatch state.
     *
     * <p>Takes the {@link OutboxStatus} enum, not a {@code String}. The field is {@code @Enumerated}, so
     * Spring Data binds this parameter as the enum; a {@code String} signature compiles and then fails at
     * runtime with {@code Argument [PENDING] of type [java.lang.String] did not match parameter type} —
     * the same defect class that a {@code String} idempotency-key lookup produced earlier in this project,
     * and one no compiler catches.
     */
    long countByStatus(OutboxStatus status);

    /**
     * Every event staged for the given aggregates, regardless of dispatch status.
     *
     * <p>Status-independent on purpose. Anything that filters on {@code PENDING} races the relay, which
     * is draining continuously — a caller asking "was an event written for this transaction?" wants the
     * answer whether or not it has already been published. Used for support lookups and for assertions
     * that must not depend on relay timing.
     */
    List<OutboxEvent> findByAggregateIdIn(Collection<UUID> aggregateIds);

    /**
     * Claims a batch of events for publication.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what allows the relay to run on more than one instance:
     * each worker takes a disjoint set of rows and never blocks on rows another worker already holds.
     * Without {@code SKIP LOCKED}, a second worker would serialise behind the first and add nothing
     * but lock contention. With plain {@code NOWAIT} it would fail instead of moving on.
     *
     * <p>Ordering by {@code (available_at, id)} matches the partial index
     * {@code idx_outbox_events_claimable} exactly, so this stays an index scan over pending rows only,
     * regardless of how much published history has accumulated.
     *
     * <p>Expressed as a native query because JPQL has no way to express {@code SKIP LOCKED}. The
     * alternative — {@code @Lock} plus a {@code jakarta.persistence.lock.timeout} hint of {@code -2} —
     * relies on a Hibernate-specific magic value, and being explicit is worth more here than
     * portability the project does not need.
     *
     * <p>Must be called inside a transaction: the row locks are held until it ends, and that is what
     * prevents another worker from re-claiming the same events mid-publication.
     */
    @Query(value = """
            select *
              from outbox_events
             where status in ('PENDING', 'FAILED')
               and available_at <= :now
             order by available_at, id
             limit :batchSize
             for update skip locked
            """, nativeQuery = true)
    List<OutboxEvent> claimPendingBatch(@Param("now") Instant now,
                                        @Param("batchSize") int batchSize);

    /**
     * Events stuck unpublished past a threshold — the relay's health signal.
     *
     * <p>A growing result means committed ledger changes are not reaching consumers, so downstream
     * projections are diverging from the ledger even though nothing has failed loudly.
     */
    @Query(value = """
            select *
              from outbox_events
             where status in ('PENDING', 'FAILED')
               and created_at < :threshold
             order by created_at
             limit :limit
            """, nativeQuery = true)
    List<OutboxEvent> findStalled(@Param("threshold") Instant threshold,
                                  @Param("limit") int limit);

    /**
     * Prunes successfully published events older than {@code threshold}.
     *
     * <p>Safe to delete: the ledger itself is the durable record, and these rows only attest that an
     * event was handed to Kafka. Retention should exceed the Kafka topic's own retention so the two
     * can still be cross-checked.
     */
    @Modifying
    @Query(value = """
            delete from outbox_events
             where status = 'PUBLISHED'
               and published_at < :threshold
            """, nativeQuery = true)
    int deletePublishedBefore(@Param("threshold") Instant threshold);
}
