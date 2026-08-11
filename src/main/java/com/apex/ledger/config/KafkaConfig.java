package com.apex.ledger.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

/**
 * Kafka topics, and the string-based producer and consumer used for the outbox pipeline.
 *
 * <h2>Why a String serializer rather than the JSON one</h2>
 *
 * <p>{@code application.yml} configures a {@code JsonSerializer} for general use, but the outbox relay
 * must not use it. An outbox payload is <em>already</em> a serialised JSON document; handing it to a
 * JSON serializer would encode it a second time, so the record on the topic would be a JSON string
 * containing escaped JSON. Consumers would have to double-decode, and the "exact bytes we committed"
 * property that makes the outbox auditable would be gone.
 *
 * <p>The consumer side mirrors this: it receives the raw document and parses it explicitly. That is also
 * what lets a consumer tolerate a producer that has added fields — it controls its own
 * {@code ObjectMapper} rather than depending on a deserializer configured for one target type.
 *
 * <h2>Topics are declared, not auto-created</h2>
 *
 * <p>The broker runs with {@code auto.create.topics.enable=false} (see {@code docker-compose.yml}), so
 * every topic must exist as a {@code NewTopic} bean. That is the point: a typo in a topic name fails
 * loudly here instead of silently creating a second topic that nothing consumes.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    /**
     * Matches the broker default in {@code docker-compose.yml}. Room to scale consumer concurrency
     * without a repartition, which on a keyed topic would break ordering for in-flight keys.
     */
    private static final int PARTITIONS = 12;

    /**
     * Single-broker local infrastructure. A deployed cluster must raise this to at least 3 with
     * {@code min.insync.replicas=2}; RF=1 means one broker loss is permanent event loss, which for a
     * ledger's audit stream is unacceptable.
     */
    private static final short REPLICATION_FACTOR = 1;

    // ------------------------------------------------------------------- topics

    @Bean
    public NewTopic journalEntriesTopic(ApexLedgerProperties properties) {
        return TopicBuilder.name(properties.topics().journalEntries())
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                // -1 = keep forever. This is the audit stream of a ledger; ageing it out would
                // discard the record of what the system published.
                .config("retention.ms", "-1")
                .config("cleanup.policy", "delete")
                .build();
    }

    @Bean
    public NewTopic balanceProjectionsTopic(ApexLedgerProperties properties) {
        return TopicBuilder.name(properties.topics().balanceProjections())
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                // Compacted: a balance projection is a snapshot per key, so only the latest matters
                // and a consumer rebuilding state needs the newest value, not the history.
                .config("cleanup.policy", "compact")
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic(ApexLedgerProperties properties) {
        return TopicBuilder.name(properties.topics().deadLetter())
                .partitions(PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .config("retention.ms", "-1")
                .build();
    }

    // ----------------------------------------------------------------- producer

    /**
     * Producer for already-serialised outbox payloads.
     *
     * <p>Inherits everything from {@code spring.kafka.producer.*} — {@code acks=all},
     * {@code enable.idempotence=true}, compression, the delivery timeout — and overrides only the value
     * serializer. Copying the full producer config here instead would let the two drift, and a
     * relay quietly running with {@code acks=1} is exactly the kind of divergence nobody notices until
     * a broker fails.
     */
    @Bean
    public ProducerFactory<String, String> outboxProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> configs = kafkaProperties.buildProducerProperties(null);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        log.info("outbox producer configured: acks={}, idempotence={}",
                configs.get(ProducerConfig.ACKS_CONFIG),
                configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(
            ProducerFactory<String, String> outboxProducerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(outboxProducerFactory);
        // Ties each send into the surrounding Micrometer trace, so a GraphQL mutation, its transaction
        // and the resulting record share one trace id.
        template.setObservationEnabled(true);
        return template;
    }

    // ----------------------------------------------------------------- consumer

    /**
     * Consumer factory delivering raw JSON strings.
     *
     * <p>Overrides the {@code ErrorHandlingDeserializer}/{@code JsonDeserializer} pair from
     * {@code application.yml}. With no type headers on the wire — the producer sends
     * {@code spring.json.add.type.headers=false}, deliberately, so consumers are not coupled to our
     * class names — a {@code JsonDeserializer} has no target type to bind to and would need a
     * configured default. Delivering the document and parsing it in the listener is both simpler and
     * more robust: a {@code StringDeserializer} cannot fail, which moves every parse failure inside the
     * listener where the error handler can route it to the DLQ.
     */
    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> configs = kafkaProperties.buildConsumerProperties(null);
        configs.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        configs.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        // These delegate settings belong to the ErrorHandlingDeserializer being replaced; leaving them
        // in place would have the client warn about configs it does not recognise on every startup.
        configs.remove("spring.deserializer.key.delegate.class");
        configs.remove("spring.deserializer.value.delegate.class");
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    /**
     * Listener container for the audit consumer.
     *
     * <p>Retries a failing record with exponential backoff and, once the attempts are exhausted,
     * publishes it to the dead-letter topic and commits past it. The alternative — retrying forever —
     * stops the partition on one bad record, which for an audit stream means every subsequent event is
     * delayed indefinitely by a single malformed one.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> stringListenerContainerFactory(
            ConsumerFactory<String, String> stringConsumerFactory,
            KafkaTemplate<String, String> outboxKafkaTemplate,
            ApexLedgerProperties properties) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);

        // Route the failed record to the DLQ, preserving its original partition so ordering within a
        // key is still inspectable there.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                outboxKafkaTemplate,
                (record, exception) -> new org.apache.kafka.common.TopicPartition(
                        properties.topics().deadLetter(), record.partition()));

        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxElapsedTime(30_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        // A record that cannot be parsed will never parse on a retry; send it straight to the DLQ
        // instead of spending the backoff budget proving it again.
        errorHandler.addNotRetryableExceptions(
                com.fasterxml.jackson.core.JsonProcessingException.class,
                IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
