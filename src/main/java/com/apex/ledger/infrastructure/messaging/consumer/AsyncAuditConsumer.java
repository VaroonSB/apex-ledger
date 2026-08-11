package com.apex.ledger.infrastructure.messaging.consumer;

import com.apex.ledger.domain.event.TransactionSettledEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * An external audit service consuming the ledger stream.
 *
 * <p>Stands in for a separate deployable — a compliance archive, a regulatory feed, a fraud pipeline —
 * and is written the way such a consumer has to be written rather than as a minimal example. The four
 * properties below are what a real downstream consumer of a ledger must get right.
 *
 * <h2>1. Deduplicate, because delivery is at-least-once</h2>
 *
 * <p>The outbox relay can publish and then fail before marking the row, so events are redelivered. A
 * consumer that counts, sums or archives without deduplicating will double-count — and for an audit
 * trail, a duplicated entry is as wrong as a missing one. Deduplication keys on the
 * {@code apex-event-id} header, which is stable across every redelivery of the same event.
 *
 * <p>The in-memory guard here is bounded and therefore only a demonstration. A real consumer needs a
 * durable dedup store — a unique constraint on {@code event_id} in the audit database is the usual
 * answer, and it is the same trick the ledger itself uses for idempotency. An in-memory set forgets
 * everything on restart, which is exactly when redelivery is most likely.
 *
 * <h2>2. Acknowledge only after the work is durable</h2>
 *
 * <p>{@code ack-mode: manual_immediate}, and {@link Acknowledgment#acknowledge()} is called only after
 * processing succeeds. Committing the offset first — which auto-commit does on a timer — acknowledges
 * records that have not been handled, and a crash in that window loses them silently. There is no
 * recovery from a committed offset for an event that was never processed.
 *
 * <h2>3. Never acknowledge a record you failed to process</h2>
 *
 * <p>An exception propagates so the container's error handler owns it: retry with backoff, then route to
 * the dead-letter topic and move on. Catching and swallowing would drop the event; retrying forever in
 * the listener would stop the partition, delaying every subsequent event behind one bad record.
 *
 * <h2>4. Tolerate a producer that has moved ahead</h2>
 *
 * <p>Parsing ignores unknown fields, so a producer adding a field does not break this consumer. Events
 * on this topic are retained indefinitely, so a consumer that fails on unrecognised fields cannot
 * replay history written by a newer producer.
 */
@Component
public class AsyncAuditConsumer {

    private static final Logger log = LoggerFactory.getLogger(AsyncAuditConsumer.class);

    /**
     * Bounded LRU of seen event ids.
     *
     * <p>Bounded on purpose: an unbounded set in a long-lived consumer of an indefinitely-retained topic
     * is a memory leak with a slow fuse. The bound means it is a cheap filter for the common
     * near-in-time redelivery, not a correctness guarantee — see the class note on durable dedup.
     */
    private static final int DEDUP_CAPACITY = 10_000;

    private final ObjectMapper objectMapper;
    private final Set<UUID> recentlySeen;

    private final Counter auditedCounter;
    private final Counter duplicateCounter;
    private final Counter malformedCounter;

    public AsyncAuditConsumer(ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");

        // Synchronized wrapper because listener concurrency is > 1: the container runs several consumer
        // threads, and they share this filter. The critical section is a map lookup with no I/O, so it
        // cannot meaningfully pin a carrier thread even though listeners run on virtual threads.
        this.recentlySeen = Collections.newSetFromMap(Collections.synchronizedMap(
                new LinkedHashMap<>(DEDUP_CAPACITY, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<UUID, Boolean> eldest) {
                        return size() > DEDUP_CAPACITY;
                    }
                }));

        this.auditedCounter = Counter.builder("apex.ledger.audit.consumed")
                .tag("outcome", "audited").register(meterRegistry);
        this.duplicateCounter = Counter.builder("apex.ledger.audit.consumed")
                .tag("outcome", "duplicate").register(meterRegistry);
        this.malformedCounter = Counter.builder("apex.ledger.audit.consumed")
                .tag("outcome", "malformed").register(meterRegistry);
    }

    /**
     * Consumes one settled transaction.
     *
     * <p>Takes the whole {@link ConsumerRecord} rather than just the value, because the headers carry the
     * event id this consumer deduplicates on and the event type it routes on — both available without
     * parsing the body.
     *
     * <p>{@code concurrency} is left to the container factory; ordering is preserved per partition, and
     * because events are keyed by transaction id, all events for one transaction land on one partition
     * and are seen in order.
     */
    @KafkaListener(
            id = "apex-ledger-audit",
            topics = "${apex.ledger.topics.journal-entries}",
            groupId = "apex-ledger-audit",
            containerFactory = "stringListenerContainerFactory")
    public void onTransactionSettled(ConsumerRecord<String, String> record,
                                     Acknowledgment acknowledgment) {

        UUID eventId = readEventId(record);
        String eventType = readHeader(record, "apex-event-type");

        // Deduplicate before parsing: a redelivery costs a set lookup rather than a JSON parse.
        if (eventId != null && !recentlySeen.add(eventId)) {
            duplicateCounter.increment();
            log.debug("ignoring redelivery of event {} ({}-{}@{})",
                    eventId, record.topic(), record.partition(), record.offset());
            acknowledgment.acknowledge();
            return;
        }

        TransactionSettledEvent event;
        try {
            event = objectMapper.readValue(record.value(), TransactionSettledEvent.class);
        } catch (JsonProcessingException e) {
            malformedCounter.increment();
            // Do NOT acknowledge. The error handler classifies JsonProcessingException as
            // non-retryable and routes the record to the dead-letter topic, which is the only sensible
            // destination: a record that cannot be parsed will not parse on a retry either.
            log.error("could not parse audit event at {}-{}@{} (eventId={}); routing to the DLQ",
                    record.topic(), record.partition(), record.offset(), eventId, e);
            throw new IllegalArgumentException(
                    "unparseable TransactionSettledEvent at offset " + record.offset(), e);
        }

        audit(record, eventId, eventType, event);

        auditedCounter.increment();
        acknowledgment.acknowledge();
    }

    /**
     * The audit action itself.
     *
     * <p>A real service would write to an append-only archive under a unique constraint on the event id.
     * Here it emits a structured line and re-checks the double-entry invariant on the received payload —
     * which is a genuinely useful thing for an independent auditor to do: it verifies the balance
     * property from outside the ledger, using only what was published, rather than trusting the producer.
     */
    private void audit(ConsumerRecord<String, String> record, UUID eventId, String eventType,
                       TransactionSettledEvent event) {

        BigDecimal net = event.entries().stream()
                .map(entry -> entry.direction().signum() < 0
                        ? entry.amount().negate()
                        : entry.amount())
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        if (net.signum() != 0) {
            // Independent detection of a broken invariant. Loud, because if this ever fires the ledger
            // published something that does not balance and the database constraints were bypassed.
            log.error("AUDIT ALERT: transaction {} published entries that do not sum to zero "
                            + "(net {}); event {} at {}-{}@{}",
                    event.transactionId(), net.toPlainString(), eventId,
                    record.topic(), record.partition(), record.offset());
            return;
        }

        log.info("AUDIT type={} event={} transaction={} kind={} legs={} effectiveAt={} "
                        + "postedAt={} by={} partition={} offset={}",
                eventType, eventId, event.transactionId(), event.kind(), event.entries().size(),
                event.effectiveAt(), event.postedAt(), event.postedBy(),
                record.partition(), record.offset());
    }

    private UUID readEventId(ConsumerRecord<String, String> record) {
        String raw = readHeader(record, "apex-event-id");
        if (raw == null) {
            // Not fatal: fall through to processing without dedup rather than discarding a real event
            // over a missing header. Worth flagging, because it means the producer changed.
            log.warn("audit event at {}-{}@{} carried no apex-event-id header; cannot deduplicate",
                    record.topic(), record.partition(), record.offset());
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            log.warn("audit event at {}-{}@{} carried a malformed apex-event-id '{}'",
                    record.topic(), record.partition(), record.offset(), raw);
            return null;
        }
    }

    private String readHeader(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Test/diagnostic accessor: how many distinct events this instance has seen recently. */
    public int recentlySeenCount() {
        return recentlySeen.size();
    }
}
