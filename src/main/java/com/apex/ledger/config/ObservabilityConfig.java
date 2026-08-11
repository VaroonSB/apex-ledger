package com.apex.ledger.config;

import io.micrometer.core.aop.CountedAspect;
import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Makes the ledger's metrics usable.
 *
 * <h2>Why the timers are not created here</h2>
 *
 * <p>The {@code Timer} and {@code Counter} instances themselves live inside the services that record
 * them — {@code LedgerEngineService}, {@code RedissonAccountLockManager}, {@code AccountBalanceCache},
 * {@code OutboxRelayService} — because a configuration class cannot time a method body. Registering
 * them centrally and passing them around would separate a meter from the code path it measures, which
 * is how meters end up recording the wrong thing after a refactor.
 *
 * <p>What this class does is make those meters answer operational questions: it attaches SLO buckets so
 * a latency percentile is meaningful, caps cardinality so the registry cannot be blown up, and enables
 * the annotation-driven aspects. The inventory below is the contract; changing a meter name in a service
 * without changing it here silently drops its histogram configuration.
 *
 * <h2>The meter inventory</h2>
 *
 * <table>
 *   <caption>Meters recorded by the core services</caption>
 *   <tr><th>Meter</th><th>Type</th><th>Recorded by</th><th>What it answers</th></tr>
 *   <tr><td>{@code apex.ledger.posting}</td><td>Timer</td><td>engine</td>
 *       <td>End-to-end posting latency, lock acquisition included</td></tr>
 *   <tr><td>{@code apex.ledger.posting.result}</td><td>Counter, tag {@code outcome}</td><td>engine</td>
 *       <td>posted / replayed / rejected — the failure rate</td></tr>
 *   <tr><td>{@code apex.ledger.lock.acquisition}</td><td>Timer</td><td>lock manager</td>
 *       <td>Lock contention: the p99 is queueing time behind a hot account</td></tr>
 *   <tr><td>{@code apex.ledger.lock.timeout}</td><td>Counter</td><td>lock manager</td>
 *       <td>Load shed because accounts were too contended</td></tr>
 *   <tr><td>{@code apex.ledger.lock.lease.expired}</td><td>Counter</td><td>lock manager</td>
 *       <td>MUST be zero. Non-zero means mutual exclusion was lost mid-posting</td></tr>
 *   <tr><td>{@code apex.ledger.balance.cache}</td><td>Counter, tag {@code result}</td><td>cache</td>
 *       <td>Hit ratio — whether the cache is actually protecting PostgreSQL</td></tr>
 *   <tr><td>{@code apex.ledger.balance.cache.stale.rejected}</td><td>Counter</td><td>cache</td>
 *       <td>Out-of-order cache writes the fence discarded</td></tr>
 *   <tr><td>{@code apex.ledger.outbox.relay}</td><td>Counter, tag {@code outcome}</td><td>relay</td>
 *       <td>published / failed / abandoned</td></tr>
 *   <tr><td>{@code apex.ledger.outbox.backlog}</td><td>Gauge</td><td>relay</td>
 *       <td>Events awaiting publication. A rising value means consumers are diverging</td></tr>
 *   <tr><td>{@code apex.ledger.audit.consumed}</td><td>Counter, tag {@code outcome}</td><td>consumer</td>
 *       <td>audited / duplicate / malformed</td></tr>
 *   <tr><td>{@code apex.ledger.api.rate.limited}</td><td>Counter</td><td>exception resolver</td>
 *       <td>Postings shed before reaching the engine. Resilience4j has no rejection counter of its
 *       own, so this is the only place shed load is visible</td></tr>
 *   <tr><td>{@code apex.ledger.invariant.*}</td><td>Gauge</td><td>invariant metrics</td>
 *       <td>Whether the ledger is RIGHT, not merely healthy. Must be 0</td></tr>
 * </table>
 */
@Configuration(proxyBeanMethods = false)
public class ObservabilityConfig {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityConfig.class);

    /** Every ledger meter shares this prefix, which is what the filters below key on. */
    private static final String LEDGER_PREFIX = "apex.ledger.";

    private static final String POSTING_TIMER = "apex.ledger.posting";
    private static final String LOCK_ACQUISITION_TIMER = "apex.ledger.lock.acquisition";
    private static final String RELAY_BATCH_TIMER = "apex.ledger.outbox.relay.batch";

    /**
     * Tag keys that must never reach the registry.
     *
     * <p>This is a guard against the single easiest way to take down a monitoring system from
     * application code. A Prometheus time series is created per distinct tag combination, so tagging a
     * meter with an account or transaction id produces one series per account — unbounded, growing
     * forever, and fatal to the scrape long before anyone notices the cause.
     *
     * <p>The ledger does not currently emit any of these. The filter exists so that if someone adds one
     * — and it is a tempting thing to add while debugging — it is dropped rather than shipped. The
     * identifiers belong in logs and traces, which are built for high cardinality; metrics are not.
     */
    private static final Set<String> FORBIDDEN_HIGH_CARDINALITY_TAGS = Set.of(
            "accountId", "account_id", "transactionId", "transaction_id",
            "idempotencyKey", "idempotency_key", "eventId", "event_id",
            "accountNumber", "account_number", "cursor", "reference");

    /**
     * SLO boundaries for the posting path, in milliseconds.
     *
     * <p>Explicit buckets rather than only percentiles, because percentiles computed client-side cannot
     * be aggregated across instances — averaging two instances' p99 is meaningless, whereas histogram
     * buckets add up correctly. These are the values a dashboard and an alert threshold read.
     */
    private static final Duration[] POSTING_SLO = {
            Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
            Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
            Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5)
    };

    /**
     * SLO boundaries for lock acquisition.
     *
     * <p>Finer at the bottom than the posting timer because an uncontended acquisition is sub-millisecond,
     * and the interesting signal is the shape of the tail: buckets bunched near the configured 2s wait
     * time are what distinguish "busy" from "about to start shedding load".
     */
    private static final Duration[] LOCK_SLO = {
            Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10),
            Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
            Duration.ofSeconds(2)
    };

    /**
     * Applies histogram configuration and the cardinality guard.
     *
     * <p>A {@link MeterRegistryCustomizer} rather than {@code management.metrics.distribution.*} in
     * {@code application.yml}: the bucket boundaries are a reasoned choice tied to the ledger's latency
     * budget, and expressing them in code lets them carry the explanation. The YAML equivalent would be
     * a bare list of numbers.
     */
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> ledgerMeterCustomizer() {
        return registry -> {
            registry.config()
                    .meterFilter(percentileHistogramFor(POSTING_TIMER, POSTING_SLO))
                    .meterFilter(percentileHistogramFor(LOCK_ACQUISITION_TIMER, LOCK_SLO))
                    .meterFilter(percentileHistogramFor(RELAY_BATCH_TIMER, POSTING_SLO))
                    .meterFilter(highCardinalityGuard());

            log.info("ledger meter filters installed: SLO histograms on {}, {}, {}; "
                            + "{} high-cardinality tag keys denied",
                    POSTING_TIMER, LOCK_ACQUISITION_TIMER, RELAY_BATCH_TIMER,
                    FORBIDDEN_HIGH_CARDINALITY_TAGS.size());
        };
    }

    /**
     * Enables percentile histograms and SLO buckets on one timer.
     *
     * <p>Scoped to a named meter rather than applied globally: a histogram multiplies a meter's series
     * count by the number of buckets, so switching it on for everything would be a self-inflicted
     * cardinality problem of the kind {@link #highCardinalityGuard()} exists to prevent.
     */
    private static MeterFilter percentileHistogramFor(String meterName, Duration[] slo) {
        return new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id,
                                                        DistributionStatisticConfig config) {
                if (!meterName.equals(id.getName())) {
                    return config;
                }
                // Timers record in nanoseconds, and the SLO API takes the base unit as double[].
                double[] sloNanos = new double[slo.length];
                for (int i = 0; i < slo.length; i++) {
                    sloNanos[i] = (double) slo[i].toNanos();
                }
                return DistributionStatisticConfig.builder()
                        .percentilesHistogram(true)
                        .serviceLevelObjectives(sloNanos)
                        // Anything slower than the top bucket is an outlier, not a latency band worth
                        // resolving; capping keeps the bucket count bounded.
                        .maximumExpectedValue((double) slo[slo.length - 1].toNanos())
                        .minimumExpectedValue((double) Duration.ofNanos(100_000).toNanos())
                        // Long enough that a low-traffic account's postings still form a usable
                        // distribution between scrapes.
                        .expiry(Duration.ofMinutes(5))
                        .bufferLength(5)
                        .build()
                        .merge(config);
            }
        };
    }

    /**
     * Denies any ledger meter carrying a forbidden tag key, loudly.
     *
     * <p>Denies the whole meter rather than stripping the tag. Dropping the tag would leave a meter that
     * looks fine and silently aggregates what the author meant to separate; denying it means the missing
     * data is noticed, and the log line says exactly why.
     */
    private static MeterFilter highCardinalityGuard() {
        return new MeterFilter() {
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                if (!id.getName().startsWith(LEDGER_PREFIX)) {
                    return MeterFilterReply.NEUTRAL;
                }
                List<String> offending = id.getTags().stream()
                        .map(io.micrometer.core.instrument.Tag::getKey)
                        .filter(FORBIDDEN_HIGH_CARDINALITY_TAGS::contains)
                        .toList();
                if (offending.isEmpty()) {
                    return MeterFilterReply.NEUTRAL;
                }
                log.error("DENIED meter '{}': tag key(s) {} are unbounded and would create one time "
                                + "series per entity. Put identifiers in logs or trace attributes, "
                                + "not in metric tags.",
                        id.getName(), offending);
                return MeterFilterReply.DENY;
            }
        };
    }

    /**
     * Enables {@code @Timed}.
     *
     * <p>Not auto-configured by Spring Boot — the aspect bean has to be declared, and without it
     * {@code @Timed} is a silent no-op. The services time their hot paths programmatically, which is
     * more precise; this exists so a method can be instrumented without threading a
     * {@code MeterRegistry} into it, which is the difference between measuring something and not
     * bothering.
     */
    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }

    /** Enables {@code @Counted}. Same reasoning as {@link #timedAspect}. */
    @Bean
    public CountedAspect countedAspect(MeterRegistry registry) {
        return new CountedAspect(registry);
    }
}
