package com.apex.ledger.config;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

/**
 * Framework-level beans that have no natural home elsewhere.
 */
@Configuration(proxyBeanMethods = false)
public class CoreBeansConfig {

    /**
     * The clock every ledger timestamp is read from.
     *
     * <p>Injected rather than taken from {@code Instant.now()} so that time is a dependency like any
     * other. In a ledger that matters more than usual: {@code effective_at}, {@code created_at} and the
     * outbox {@code occurred_at} are all audit data, and a test that needs to assert on a backdated
     * correction or on retry-window behaviour has to be able to control the clock rather than sleep.
     *
     * <p>Fixed to UTC. The database stores {@code TIMESTAMPTZ} and Hibernate is configured with
     * {@code hibernate.jdbc.time_zone=UTC}; a JVM default zone leaking into a ledger timestamp is the
     * kind of bug that only shows up when the deployment moves region.
     *
     * <p>Note that this is <em>not</em> the authority for ordering. PostgreSQL's commit timestamps are
     * ({@code track_commit_timestamp=on}), because application clocks across nodes disagree.
     */
    @Bean
    public Clock clock() {
        // Ticks at microsecond resolution, matching what PostgreSQL TIMESTAMPTZ stores.
        //
        // Without this, an Instant carries nanoseconds that the database silently drops, so an
        // in-memory entity and the same row re-read disagree on their own timestamps. That is
        // invisible until something compares them — an equality assertion, a digest over an audit
        // record, a fingerprint. Truncating at the source means a ledger timestamp is always exactly
        // representable in the store it is going to.
        return Clock.tick(Clock.systemUTC(), Duration.ofNanos(1_000));
    }

    /**
     * Makes Jackson's tree model preserve {@link java.math.BigDecimal} scale.
     *
     * <p>This closes a money-corruption path that {@code spring.jackson.*} alone does not. By default
     * {@code JsonNodeFactory} calls {@code stripTrailingZeros()} on every decimal it builds, so reading
     * {@code {"amount":100.00}} into a {@code JsonNode} yields {@code 1E+2} — a {@code BigDecimal} with
     * scale {@code -2}. Re-serialising that node emits {@code {"amount":1E+2}}.
     *
     * <p>{@code WRITE_BIGDECIMAL_AS_PLAIN} does not save it: the scale is already gone by then, so
     * "plain" renders {@code 100} and the two cent digits that encode the currency's minor unit are
     * lost. Anything that round-trips JSON through the tree model is affected — the outbox relay if it
     * ever parses a payload, a GraphQL scalar working on {@code JsonNode}, a diagnostic that reads a
     * stored event and writes it back.
     *
     * <p>Fixed once, globally, rather than left to each call site to remember.
     */
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer exactBigDecimalNodesCustomizer() {
        return builder -> builder.postConfigurer(
                mapper -> mapper.setNodeFactory(new JsonNodeFactory(true)));
    }
}
