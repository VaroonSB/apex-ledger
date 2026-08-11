package com.apex.ledger.integration;

import com.apex.ledger.api.graphql.scalar.LedgerScalars;
import com.apex.ledger.api.graphql.support.EntryCursor;
import com.apex.ledger.domain.event.TransactionSettledEvent;
import com.apex.ledger.domain.model.AccountType;
import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.domain.model.OutboxStatus;
import com.apex.ledger.infrastructure.messaging.producer.OutboxRelayService;
import com.apex.ledger.infrastructure.persistence.entity.Account;
import com.apex.ledger.infrastructure.persistence.entity.OutboxEvent;
import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import com.apex.ledger.infrastructure.persistence.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.CoercingParseValueException;
import graphql.schema.GraphQLScalarType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.graphql.tester.AutoConfigureGraphQlTester;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.springframework.graphql.test.tester.GraphQlTester;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end verification of the Phase 4 API and event pipeline.
 *
 * <p>Walks the whole path a transfer takes: a GraphQL mutation, through the engine, into the outbox in
 * the same commit, out via the relay to Kafka, and into the audit consumer — asserting at each hop
 * rather than trusting the one before it.
 *
 * <p>Also covers what only a running stack can show: that {@code Decimal} reaches the wire as a
 * plain-notation string, that domain exceptions arrive as typed GraphQL errors with stable
 * {@code errorCode} extensions, that keyset cursors stay valid while new postings land ahead of them,
 * and that the relay publishes the committed payload byte-for-byte.
 *
 * <p><strong>Kafka is in-process, not a container.</strong> {@code @EmbeddedKafka} with
 * {@code kraft = true} runs a real broker inside the JVM, which is both faster than a container and one
 * less moving part in CI. PostgreSQL and Redis do use Testcontainers, because there is no comparable
 * in-process equivalent for either. The {@code org.testcontainers:kafka} module stays declared in the
 * POM for a future test that needs a broker outside the JVM — a restart, a network partition — which is
 * the one thing an embedded broker cannot simulate.
 */
@Testcontainers
@SpringBootTest
@AutoConfigureGraphQlTester
@EmbeddedKafka(
        kraft = true,
        partitions = 1,
        topics = {"apex.ledger.journal-entries.v1", "apex.ledger.dlq.v1"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class LedgerApiAndEventsIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16.14-alpine")
                    .withDatabaseName("apex_ledger")
                    .withUsername("apex")
                    .withPassword("apex_local_test");

    @Container
    @ServiceConnection
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.10-alpine"))
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--maxmemory", "256mb",
                            "--maxmemory-policy", "noeviction", "--save", "");


    private static final String POST = """
            mutation Post($input: PostTransactionInput!) {
              postTransaction(input: $input) {
                replayed
                transaction { id kind idempotencyKey reference effectiveAt createdBy
                              entries { entrySequence direction amount currency signedAmount } }
                balancesAfter { accountId currency balance totalDebits totalCredits }
              }
            }
            """;

    @Autowired GraphQlTester graphQl;
    @Autowired AccountRepository accounts;
    @Autowired OutboxEventRepository outbox;
    @Autowired OutboxRelayService relay;
    @Autowired TransactionTemplate tx;
    @Autowired ObjectMapper objectMapper;
    @Autowired EmbeddedKafkaBroker broker;

    private String uniq() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private Account open(AccountType type, Money floor) {
        return tx.execute(s -> accounts.save(Account.open(
                "ACC-" + uniq(), "T " + uniq(), type, CurrencyCode.of("USD"), floor, Instant.now())));
    }

    private Map<String, Object> input(Account source, Account destination, String amount, String key) {
        return Map.of(
                "sourceAccountId", source.getId().toString(),
                "destinationAccountId", destination.getId().toString(),
                "amount", amount,
                "currency", "USD",
                "idempotencyKey", key,
                "reference", "ref-" + key,
                "description", "graphql test");
    }

    // -------------------------------------------------------- 1. mutation & schema

    @Test
    void postTransaction_commits_and_returns_the_full_payload() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "gql-" + uniq();

        GraphQlTester.Response response = graphQl.document(POST)
                .variable("input", input(customer, cash, "100.00", key))
                .execute();

        response.path("postTransaction.replayed").entity(Boolean.class).isEqualTo(false);
        response.path("postTransaction.transaction.kind").entity(String.class).isEqualTo("TRANSFER");
        response.path("postTransaction.transaction.idempotencyKey").entity(String.class).isEqualTo(key);

        // Decimal is serialised as a STRING in plain notation, so a JS client cannot coerce a balance
        // into an IEEE double.
        List<String> amounts = response.path("postTransaction.transaction.entries[*].amount")
                .entityList(String.class).get();
        assertThat(amounts).containsExactly("100.00", "100.00");

        // source is CREDITED, destination is DEBITED — the documented convention.
        List<String> directions = response.path("postTransaction.transaction.entries[*].direction")
                .entityList(String.class).get();
        assertThat(directions).containsExactly("DEBIT", "CREDIT");
        List<String> signed = response.path("postTransaction.transaction.entries[*].signedAmount")
                .entityList(String.class).get();
        assertThat(signed).containsExactly("100.00", "-100.00");

        response.path("postTransaction.balancesAfter").entityList(Object.class).hasSize(2);
    }

    @Test
    void identical_resubmission_is_reported_as_a_replay_not_an_error() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "gql-" + uniq();
        Map<String, Object> variables = input(customer, cash, "10.00", key);

        String firstId = graphQl.document(POST).variable("input", variables).execute()
                .path("postTransaction.transaction.id").entity(String.class).get();

        GraphQlTester.Response replay =
                graphQl.document(POST).variable("input", variables).execute();
        replay.path("postTransaction.replayed").entity(Boolean.class).isEqualTo(true);
        replay.path("postTransaction.transaction.id").entity(String.class).isEqualTo(firstId);
        // No new balances are invented for a replay.
        replay.path("postTransaction.balancesAfter").entityList(Object.class).hasSize(0);
    }

    // ------------------------------------------------------------ 2. error mapping

    @Test
    void reusing_a_key_with_a_different_amount_is_a_typed_graphql_error() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "gql-" + uniq();
        graphQl.document(POST).variable("input", input(customer, cash, "10.00", key)).executeAndVerify();

        graphQl.document(POST).variable("input", input(customer, cash, "999.00", key))
                .execute()
                .errors()
                .satisfy(errors -> {
                    assertThat(errors).hasSize(1);
                    Map<String, Object> extensions = errors.get(0).getExtensions();
                    assertThat(extensions.get("errorCode")).isEqualTo("IDEMPOTENCY_KEY_REUSED");
                    assertThat(extensions.get("retryable")).isEqualTo(false);
                    assertThat(extensions).containsKey("existingTransactionId");
                    assertThat(errors.get(0).getErrorType().toString()).isEqualTo("BAD_REQUEST");
                });
    }

    @Test
    void overdraft_is_mapped_to_insufficient_funds_and_marked_non_retryable() {
        Account floored = open(AccountType.ASSET, Money.zero(CurrencyCode.of("USD")));
        Account other = open(AccountType.ASSET, null);

        // Crediting a debit-normal account drives its balance negative, below the zero floor.
        graphQl.document(POST).variable("input", input(floored, other, "10.00", "gql-" + uniq()))
                .execute()
                .errors()
                .satisfy(errors -> {
                    Map<String, Object> extensions = errors.get(0).getExtensions();
                    assertThat(extensions.get("errorCode")).isEqualTo("INSUFFICIENT_FUNDS");
                    assertThat(extensions.get("retryable")).isEqualTo(false);
                });
    }

    @Test
    void an_amount_finer_than_the_currency_allows_is_rejected_as_invalid_input() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);

        graphQl.document(POST).variable("input", input(customer, cash, "1.005", "gql-" + uniq()))
                .execute()
                .errors()
                .satisfy(errors -> {
                    Map<String, Object> extensions = errors.get(0).getExtensions();
                    assertThat(extensions.get("errorCode")).isEqualTo("INVALID_INPUT");
                    assertThat(errors.get(0).getMessage()).contains("more precise");
                });
    }

    @Test
    void a_forged_cursor_is_rejected_without_revealing_the_format() {
        Account cash = open(AccountType.ASSET, null);
        graphQl.document("""
                        query History($id: ID!, $after: String) {
                          getTransactionHistory(accountId: $id, after: $after) {
                            edges { cursor }
                          }
                        }
                        """)
                .variable("id", cash.getId().toString())
                .variable("after", "not-a-real-cursor")
                .execute()
                .errors()
                .satisfy(errors -> {
                    Map<String, Object> extensions = errors.get(0).getExtensions();
                    assertThat(extensions.get("errorCode")).isEqualTo("INVALID_CURSOR");
                    assertThat(errors.get(0).getMessage()).contains("pageInfo.endCursor");
                });
    }

    @Test
    void unknown_account_balance_is_null_rather_than_an_error() {
        graphQl.document("query { getAccountBalance(accountId: \"%s\") { balance } }"
                        .formatted(UUID.randomUUID()))
                .execute()
                .path("getAccountBalance").valueIsNull();
    }

    // --------------------------------------------------------------- 3. pagination

    @Test
    void cursor_pagination_walks_the_statement_without_gaps_or_repeats() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        int postings = 7;
        for (int i = 0; i < postings; i++) {
            graphQl.document(POST)
                    .variable("input", input(customer, cash, "1.00", "page-" + uniq()))
                    .executeAndVerify();
        }

        String history = """
                query History($id: ID!, $first: Int, $after: String) {
                  getTransactionHistory(accountId: $id, first: $first, after: $after) {
                    edges { cursor node { id amount direction } }
                    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
                  }
                }
                """;

        List<String> seen = new ArrayList<>();
        String after = null;
        int pages = 0;
        boolean hasNext = true;
        while (hasNext && pages < 10) {
            GraphQlTester.Request<?> request = graphQl.document(history)
                    .variable("id", cash.getId().toString())
                    .variable("first", 3);
            if (after != null) {
                request = request.variable("after", after);
            }
            GraphQlTester.Response page = request.execute();

            seen.addAll(page.path("getTransactionHistory.edges[*].node.id")
                    .entityList(String.class).get());
            hasNext = page.path("getTransactionHistory.pageInfo.hasNextPage")
                    .entity(Boolean.class).get();
            // hasPreviousPage is true exactly when a cursor was supplied.
            page.path("getTransactionHistory.pageInfo.hasPreviousPage")
                    .entity(Boolean.class).isEqualTo(after != null);
            after = page.path("getTransactionHistory.pageInfo.endCursor").entity(String.class).get();
            pages++;
        }

        // 7 entries at 3 per page = 3 pages, every entry seen exactly once.
        assertThat(pages).isEqualTo(3);
        assertThat(seen).hasSize(postings);
        assertThat(new HashSet<>(seen)).hasSize(postings);
    }

    @Test
    void a_cursor_stays_valid_when_new_postings_arrive_ahead_of_it() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        for (int i = 0; i < 4; i++) {
            graphQl.document(POST).variable("input", input(customer, cash, "1.00", "stab-" + uniq()))
                    .executeAndVerify();
        }

        String history = """
                query History($id: ID!, $first: Int, $after: String) {
                  getTransactionHistory(accountId: $id, first: $first, after: $after) {
                    edges { node { id } }
                    pageInfo { endCursor }
                  }
                }
                """;

        GraphQlTester.Response first = graphQl.document(history)
                .variable("id", cash.getId().toString()).variable("first", 2).execute();
        List<String> firstPage = first.path("getTransactionHistory.edges[*].node.id")
                .entityList(String.class).get();
        String cursor = first.path("getTransactionHistory.pageInfo.endCursor")
                .entity(String.class).get();

        // Two more postings land at the HEAD of the statement while the client holds a cursor. With
        // offset pagination these would shift the window and page 2 would repeat rows from page 1.
        for (int i = 0; i < 2; i++) {
            graphQl.document(POST).variable("input", input(customer, cash, "1.00", "stab-" + uniq()))
                    .executeAndVerify();
        }

        List<String> secondPage = graphQl.document(history)
                .variable("id", cash.getId().toString()).variable("first", 2)
                .variable("after", cursor).execute()
                .path("getTransactionHistory.edges[*].node.id").entityList(String.class).get();

        assertThat(secondPage).doesNotContainAnyElementsOf(firstPage);
    }

    @Test
    void page_size_is_capped_regardless_of_what_the_client_asks_for() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        for (int i = 0; i < 3; i++) {
            graphQl.document(POST).variable("input", input(customer, cash, "1.00", "cap-" + uniq()))
                    .executeAndVerify();
        }
        // first: 1000000 must not become a full-table read.
        graphQl.document("""
                        query History($id: ID!) {
                          getTransactionHistory(accountId: $id, first: 1000000) { edges { cursor } }
                        }
                        """)
                .variable("id", cash.getId().toString())
                .execute()
                .path("getTransactionHistory.edges").entityList(Object.class).hasSize(3);
    }

    // ------------------------------------------------------------ 4. batch loading

    @Test
    void statement_lines_resolve_their_transaction_through_the_batch_loader() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        for (int i = 0; i < 5; i++) {
            graphQl.document(POST).variable("input", input(customer, cash, "2.00", "batch-" + uniq()))
                    .executeAndVerify();
        }

        List<String> references = graphQl.document("""
                        query History($id: ID!) {
                          getTransactionHistory(accountId: $id, first: 5) {
                            edges { node { id transaction { id reference entries { direction } } } }
                          }
                        }
                        """)
                .variable("id", cash.getId().toString())
                .execute()
                .path("getTransactionHistory.edges[*].node.transaction.reference")
                .entityList(String.class).get();

        assertThat(references).hasSize(5).allSatisfy(ref -> assertThat(ref).startsWith("ref-batch-"));
    }

    // -------------------------------------------------- 5. outbox relay -> Kafka

    @Test
    void relay_publishes_the_committed_payload_verbatim_and_marks_the_row_published() throws Exception {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "relay-" + uniq();

        String transactionId = graphQl.document(POST)
                .variable("input", input(customer, cash, "250.00", key))
                .execute().path("postTransaction.transaction.id").entity(String.class).get();

        OutboxEvent staged = outbox.findStalled(Instant.now().plusSeconds(120), 500).stream()
                .filter(event -> event.getAggregateId().toString().equals(transactionId))
                .findFirst().orElseThrow();
        assertThat(staged.getStatus()).isEqualTo(OutboxStatus.PENDING);
        String committedPayload = staged.getPayload();

        try (Consumer<String, String> consumer = testConsumer("relay-check-" + uniq())) {
            broker.consumeFromEmbeddedTopics(consumer, "apex.ledger.journal-entries.v1");

            int published = relay.publishBatch();
            assertThat(published).isGreaterThanOrEqualTo(1);

            ConsumerRecord<String, String> record = findRecordFor(consumer, transactionId);
            assertThat(record).as("a record for the posted transaction").isNotNull();

            // Keyed by transaction id, so all events for one transaction share a partition.
            assertThat(record.key()).isEqualTo(transactionId);

            // Headers let a consumer deduplicate and route without parsing the body.
            assertThat(header(record, "apex-event-id")).isEqualTo(staged.getEventId().toString());
            assertThat(header(record, "apex-event-type")).isEqualTo("TransactionSettled");
            assertThat(header(record, "apex-aggregate-type")).isEqualTo("Transaction");
            assertThat(header(record, "apex-aggregate-id")).isEqualTo(transactionId);

            // Published verbatim: not re-serialised through this service's Jackson config.
            assertThat(record.value()).isEqualTo(committedPayload);

            // Parseable by an independent consumer, with money precision intact.
            TransactionSettledEvent event =
                    objectMapper.readValue(record.value(), TransactionSettledEvent.class);
            assertThat(event.transactionId().toString()).isEqualTo(transactionId);
            assertThat(event.entries()).hasSize(2);
            assertThat(event.entries().get(0).amount()).isEqualByComparingTo("250.00");
            assertThat(event.entries().get(0).amount().scale()).isEqualTo(2);
        }

        OutboxEvent afterRelay = outbox.findByEventId(staged.getEventId()).orElseThrow();
        assertThat(afterRelay.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(afterRelay.getPublishedAt()).isPresent();
        assertThat(afterRelay.getAttempts()).isEqualTo(1);
    }

    @Test
    void a_published_event_is_not_relayed_again() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        graphQl.document(POST).variable("input", input(customer, cash, "5.00", "once-" + uniq()))
                .executeAndVerify();

        int firstPass = relay.publishBatch();
        assertThat(firstPass).isGreaterThanOrEqualTo(1);

        // Everything claimable has been published, so a second pass finds nothing.
        assertThat(relay.publishBatch()).isZero();
    }

    // ---------------------------------------------------------- 6. audit consumer

    @Test
    void audit_consumer_receives_the_settled_event() {
        Account cash = open(AccountType.ASSET, null);
        Account customer = open(AccountType.LIABILITY, null);
        String key = "audit-" + uniq();

        String transactionId = graphQl.document(POST)
                .variable("input", input(customer, cash, "42.00", key))
                .execute().path("postTransaction.transaction.id").entity(String.class).get();

        relay.publishBatch();

        // The @KafkaListener consumes asynchronously; poll the consumer's dedup set until it lands.
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    try (Consumer<String, String> consumer = testConsumer("audit-check-" + uniq())) {
                        broker.consumeFromEmbeddedTopics(consumer, "apex.ledger.journal-entries.v1");
                        assertThat(findRecordFor(consumer, transactionId)).isNotNull();
                    }
                });

        // Nothing landed in the dead-letter topic: the event parsed and audited cleanly.
        try (Consumer<String, String> dlq = testConsumer("dlq-check-" + uniq())) {
            broker.consumeFromEmbeddedTopics(dlq, "apex.ledger.dlq.v1");
            ConsumerRecords<String, String> records = dlq.poll(Duration.ofMillis(1500));
            assertThat(records.count()).isZero();
        }
    }

    // ------------------------------------------------------------- 7. Decimal scalar

    @Test
    void decimal_scalar_refuses_a_binary_float_but_accepts_exact_forms() {
        GraphQLScalarType decimal = LedgerScalars.decimal();
        var coercing = decimal.getCoercing();

        // The rule that matters: a double has already lost the precision money depends on.
        assertThatThrownBy(() -> coercing.parseValue(100.005d, null, null))
                .isInstanceOf(CoercingParseValueException.class)
                .hasMessageContaining("already lost the precision");
        assertThatThrownBy(() -> coercing.parseValue(1.5f, null, null))
                .isInstanceOf(CoercingParseValueException.class);

        assertThat(coercing.parseValue("100.00", null, null))
                .isEqualTo(new BigDecimal("100.00"));
        assertThat(coercing.parseValue(new BigDecimal("0.10"), null, null))
                .isEqualTo(new BigDecimal("0.10"));
        assertThat(coercing.parseValue(7, null, null)).isEqualTo(BigDecimal.valueOf(7));

        // Serialised in plain notation, never scientific.
        assertThat(coercing.serialize(new BigDecimal("1E+3"), null, null)).isEqualTo("1000");
        assertThat(coercing.serialize(new BigDecimal("1000.00"), null, null)).isEqualTo("1000.00");
    }

    @Test
    void cursor_round_trips_and_rejects_tampering() {
        EntryCursor cursor = new EntryCursor(
                Instant.parse("2026-08-11T13:45:12.123456Z"), UUID.randomUUID());
        assertThat(EntryCursor.decode(cursor.encode())).isEqualTo(cursor);
        assertThatThrownBy(() -> EntryCursor.decode("Zm9v"))
                .isInstanceOf(EntryCursor.InvalidCursorException.class);
    }

    // -------------------------------------------------------------------- helpers

    private Consumer<String, String> testConsumer(String group) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                broker.getBrokersAsString(), group, "true");
        props.put("auto.offset.reset", "earliest");
        // KafkaTestUtils defaults the key deserializer to Integer; ours are UUID strings.
        props.put("key.deserializer",
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put("value.deserializer",
                org.apache.kafka.common.serialization.StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
    }

    private ConsumerRecord<String, String> findRecordFor(Consumer<String, String> consumer,
                                                         String transactionId) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (transactionId.equals(record.key())) {
                    return record;
                }
            }
        }
        return null;
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(),
                java.nio.charset.StandardCharsets.UTF_8);
    }
}
