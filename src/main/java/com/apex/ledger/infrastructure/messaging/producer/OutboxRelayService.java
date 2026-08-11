package com.apex.ledger.infrastructure.messaging.producer;

import com.apex.ledger.config.ApexLedgerProperties;
import com.apex.ledger.infrastructure.persistence.entity.OutboxEvent;
import com.apex.ledger.infrastructure.persistence.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Drains {@code outbox_events} to Kafka.
 *
 * <h2>Why this exists at all</h2>
 *
 * <p>Publishing from inside the posting transaction cannot be made correct. Send before commit and you
 * can publish an event for a transfer that then rolls back; commit before sending and you can commit a
 * transfer whose event never reaches the broker. Either way downstream balances silently diverge from
 * the ledger. The outbox makes the event part of the same commit, and this relay moves it afterwards —
 * so an event exists if and only if the ledger change committed.
 *
 * <h2>Delivery is at-least-once, and that is inherent</h2>
 *
 * <p>The relay can publish successfully and then fail before marking the row, in which case the event is
 * published again. Claiming a database row and acknowledging a Kafka write cannot be made atomic, so
 * exactly-once is not available at this boundary. Consumers must deduplicate on the {@code eventId}
 * header, which is stable across every redelivery.
 *
 * <h2>Concurrency across instances</h2>
 *
 * <p>Every application instance runs this. {@link OutboxEventRepository#claimPendingBatch} uses
 * {@code FOR UPDATE SKIP LOCKED}, so workers take disjoint batches and never queue behind one another.
 * No leader election, no scheduler lock — the database does the coordination.
 *
 * <h2>Ordering</h2>
 *
 * <p>Rows are claimed and sent in primary-key order, and the producer is idempotent with
 * {@code max.in.flight.requests.per.connection=5}, which preserves per-partition order. Two caveats
 * worth stating: identity values have gaps (a rolled-back transaction still consumes one), so a missing
 * id is normal and must never be treated as a fault; and events are keyed by transaction, so there is no
 * global per-account ordering across partitions.
 */
@Service
public class OutboxRelayService {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);

    /** Header names. Kept as headers so a consumer can route and deduplicate without parsing the body. */
    static final String HEADER_EVENT_ID = "apex-event-id";
    static final String HEADER_EVENT_TYPE = "apex-event-type";
    static final String HEADER_AGGREGATE_TYPE = "apex-aggregate-type";
    static final String HEADER_AGGREGATE_ID = "apex-aggregate-id";
    static final String HEADER_OCCURRED_AT = "apex-occurred-at";

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    private final int batchSize;
    private final Duration sendTimeout;
    private final int maxAttempts;
    private final Duration retryBaseDelay;
    private final Duration retryMaxDelay;

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Counter abandonedCounter;
    private final Timer relayTimer;

    public OutboxRelayService(OutboxEventRepository outbox,
                              KafkaTemplate<String, String> outboxKafkaTemplate,
                              Clock clock,
                              MeterRegistry meterRegistry,
                              ApexLedgerProperties properties,
                              @Value("${apex.ledger.outbox.batch-size:100}") int batchSize,
                              @Value("${apex.ledger.outbox.send-timeout:10s}") Duration sendTimeout,
                              @Value("${apex.ledger.outbox.max-attempts:10}") int maxAttempts,
                              @Value("${apex.ledger.outbox.retry-base-delay:2s}") Duration retryBaseDelay,
                              @Value("${apex.ledger.outbox.retry-max-delay:5m}") Duration retryMaxDelay) {
        this.outbox = Objects.requireNonNull(outbox);
        this.kafkaTemplate = Objects.requireNonNull(outboxKafkaTemplate);
        this.clock = Objects.requireNonNull(clock);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.batchSize = batchSize;
        this.sendTimeout = sendTimeout;
        this.maxAttempts = maxAttempts;
        this.retryBaseDelay = retryBaseDelay;
        this.retryMaxDelay = retryMaxDelay;
        Objects.requireNonNull(properties, "properties must not be null");

        this.publishedCounter = Counter.builder("apex.ledger.outbox.relay")
                .tag("outcome", "published").register(meterRegistry);
        this.failedCounter = Counter.builder("apex.ledger.outbox.relay")
                .tag("outcome", "failed").register(meterRegistry);
        this.abandonedCounter = Counter.builder("apex.ledger.outbox.relay")
                .tag("outcome", "abandoned").register(meterRegistry);
        this.relayTimer = Timer.builder("apex.ledger.outbox.relay.batch")
                .description("Time to claim, publish and mark one outbox batch")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    /**
     * One relay tick.
     *
     * <p>{@code fixedDelay}, not {@code fixedRate}: the delay is measured from the <em>end</em> of the
     * previous run, so a slow batch cannot cause ticks to overlap and pile up. With
     * {@code spring.threads.virtual.enabled=true} this runs on a virtual thread, so blocking on Kafka
     * costs no platform thread.
     *
     * <p>Exceptions are caught and logged rather than propagated. An exception escaping a
     * {@code @Scheduled} method is logged by Spring but the schedule continues, so propagating would
     * only lose the context — and this loop must keep running through a Kafka outage.
     */
    @Scheduled(
            fixedDelayString = "${apex.ledger.outbox.poll-interval:500ms}",
            initialDelayString = "${apex.ledger.outbox.initial-delay:5s}")
    public void relayPendingEvents() {
        long startNanos = System.nanoTime();
        try {
            int published = publishBatch();
            if (published > 0) {
                log.debug("relayed {} outbox event(s)", published);
            }
        } catch (RuntimeException e) {
            // Nothing is lost: unmarked rows stay claimable and the next tick retries them.
            log.error("outbox relay tick failed; rows remain claimable and will be retried", e);
        } finally {
            relayTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Claims a batch, publishes it, and records the outcome — all in one transaction.
     *
     * <p>The transaction boundary is what makes concurrent relays safe: the {@code SKIP LOCKED} row locks
     * are held until it ends, so no other worker can claim these rows while they are in flight.
     *
     * <p>{@code REQUIRES_NEW} so a tick is never enlisted in some caller's transaction — this is invoked
     * from a scheduler, but it is also public and a future caller inside a transaction would otherwise
     * extend that transaction across a Kafka round trip.
     *
     * <p>Sends are issued for the whole batch <em>before</em> any result is awaited. Awaiting each send
     * in turn would serialise the batch into one network round trip per record and defeat the producer's
     * batching; issuing them all first lets the producer coalesce them, then the results are collected.
     *
     * @return how many events were published successfully
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int publishBatch() {
        Instant now = clock.instant();
        List<OutboxEvent> claimed = outbox.claimPendingBatch(now, batchSize);
        if (claimed.isEmpty()) {
            return 0;
        }

        List<InFlight> inFlight = new ArrayList<>(claimed.size());
        for (OutboxEvent event : claimed) {
            try {
                inFlight.add(new InFlight(event, kafkaTemplate.send(toRecord(event))));
            } catch (RuntimeException e) {
                // A synchronous failure — buffer exhausted, metadata unavailable, serialization —
                // before the record was ever handed to the producer.
                inFlight.add(new InFlight(event, CompletableFuture.failedFuture(e)));
            }
        }

        int published = 0;
        for (InFlight pending : inFlight) {
            if (awaitAndMark(pending, now)) {
                published++;
            }
        }
        return published;
    }

    /**
     * Waits for one send and updates the row accordingly.
     *
     * @return true when the event was published
     */
    private boolean awaitAndMark(InFlight pending, Instant now) {
        OutboxEvent event = pending.event();
        try {
            SendResult<String, String> result =
                    pending.future().get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
            event.markPublished(clock.instant());
            outbox.save(event);
            publishedCounter.increment();
            log.debug("published event {} to {}-{} at offset {}",
                    event.getEventId(),
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            return true;
        } catch (InterruptedException e) {
            // Shutdown. Leave the row claimable and stop touching it.
            Thread.currentThread().interrupt();
            log.warn("interrupted while publishing event {}; it stays claimable",
                    event.getEventId());
            return false;
        } catch (TimeoutException e) {
            recordFailure(event, now, "send timed out after " + sendTimeout);
            return false;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            recordFailure(event, now, cause.getClass().getSimpleName() + ": " + cause.getMessage());
            return false;
        }
    }

    /**
     * Marks a failed publication, scheduling the next attempt with exponential backoff.
     *
     * <p>Past {@code maxAttempts} the event is abandoned rather than retried forever. That is a
     * deliberate, loud giving-up: an abandoned event means a committed ledger change that downstream
     * consumers will never see, so it needs an operator, not another retry. Retrying indefinitely would
     * bury the same problem behind an ever-growing backlog.
     */
    private void recordFailure(OutboxEvent event, Instant now, String reason) {
        int attemptsAfterThis = event.getAttempts() + 1;
        if (attemptsAfterThis >= maxAttempts) {
            event.abandon("abandoned after %d attempts; last error: %s"
                    .formatted(attemptsAfterThis, reason));
            outbox.save(event);
            abandonedCounter.increment();
            log.error("ABANDONED outbox event {} ({}) for aggregate {} after {} attempts: {}. "
                            + "A committed ledger change will not reach consumers; this needs "
                            + "operator attention.",
                    event.getEventId(), event.getEventType(), event.getAggregateId(),
                    attemptsAfterThis, reason);
            return;
        }

        Instant retryAt = now.plus(backoffFor(attemptsAfterThis));
        event.markFailed(reason, retryAt);
        outbox.save(event);
        failedCounter.increment();
        log.warn("failed to publish outbox event {} (attempt {}/{}): {}. Retrying after {}.",
                event.getEventId(), attemptsAfterThis, maxAttempts, reason, retryAt);
    }

    /**
     * Exponential backoff, capped.
     *
     * <p>Doubling from the base delay, clamped at {@code retryMaxDelay} so a long outage settles into a
     * steady retry cadence instead of pushing the next attempt hours away — which would leave events
     * unpublished long after the broker recovered. The shift is bounded to avoid overflowing.
     */
    private Duration backoffFor(int attempt) {
        int exponent = Math.min(attempt - 1, 16);
        Duration candidate = retryBaseDelay.multipliedBy(1L << exponent);
        return candidate.compareTo(retryMaxDelay) > 0 ? retryMaxDelay : candidate;
    }

    /**
     * Builds the Kafka record.
     *
     * <p>The payload goes out <strong>verbatim</strong> — the exact document that was committed with the
     * ledger change. Deserialising and re-serialising it here would be worse than pointless: it would
     * make the published bytes depend on this service's current Jackson configuration rather than on
     * what was committed, so an event replayed from the outbox months later could differ from the one
     * originally published.
     */
    private ProducerRecord<String, String> toRecord(OutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                event.getTopic(), null, event.getPartitionKey(), event.getPayload());

        record.headers()
                .add(header(HEADER_EVENT_ID, event.getEventId().toString()))
                .add(header(HEADER_EVENT_TYPE, event.getEventType()))
                .add(header(HEADER_AGGREGATE_TYPE, event.getAggregateType()))
                .add(header(HEADER_AGGREGATE_ID, event.getAggregateId().toString()))
                .add(header(HEADER_OCCURRED_AT, event.getOccurredAt().toString()));

        return record;
    }

    private static RecordHeader header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Reports how far behind the relay is.
     *
     * <p>Registered as a gauge so a growing backlog is alertable. This is the signal that matters for the
     * outbox pattern: postings can be committing perfectly while nothing reaches consumers, and no error
     * rate would show it.
     */
    @Scheduled(fixedDelayString = "${apex.ledger.outbox.backlog-probe-interval:30s}")
    public void probeBacklog() {
        try {
            long stalled = outbox.findStalled(
                    clock.instant().minus(Duration.ofMinutes(1)), 1_000).size();
            meterRegistry.gauge("apex.ledger.outbox.backlog", stalled);
            if (stalled > 0) {
                log.warn("{} outbox event(s) have been awaiting publication for over a minute",
                        stalled);
            }
        } catch (RuntimeException e) {
            log.warn("could not probe the outbox backlog", e);
        }
    }

    /** One issued send and the row it belongs to. */
    private record InFlight(OutboxEvent event,
                            CompletableFuture<SendResult<String, String>> future) {
    }
}
