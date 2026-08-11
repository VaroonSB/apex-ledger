package com.apex.ledger.config;

import io.github.resilience4j.micrometer.tagged.TaggedRateLimiterMetrics;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Rate limiting for the posting mutation.
 *
 * <h2>What this protects, and what it does not</h2>
 *
 * <p>The engine's real capacity limit is the 32-connection JDBC pool, and past that the row locks on
 * contended accounts. Under a spike, requests arriving faster than the pool can retire them queue on
 * connection acquisition, time out at three seconds, and are retried by clients — which adds load to an
 * already-saturated system. The limiter's job is to convert that spiral into a fast, cheap rejection
 * before a request has consumed a connection, a distributed lock, or a database transaction.
 *
 * <p>It is <strong>not</strong> a correctness control. Nothing about the ledger's invariants depends on
 * it; it is admission control, and every guarantee still holds with it disabled.
 *
 * <h2>Two properties worth being explicit about</h2>
 *
 * <p><b>The limit is per instance, not per cluster.</b> Resilience4j keeps its permit state in the JVM,
 * so N replicas admit N × {@link #PERMITS_PER_PERIOD} per second. That is the correct thing for
 * protecting <em>this</em> process — each instance is defending its own connection pool — but it is not
 * a global quota. A cluster-wide limit needs shared state, and Redisson's {@code RRateLimiter} is
 * already available here; the reason not to reach for it is that a distributed limiter puts a Redis round
 * trip in front of every mutation and fails open or closed when Redis is unavailable, which is a worse
 * trade for a per-process protection mechanism.
 *
 * <p><b>Rejection is immediate.</b> {@link #TIMEOUT_DURATION} is zero, so a caller that cannot get a
 * permit is refused rather than parked. Waiting would convert a throughput problem into a latency
 * problem, and for a posting API that is actively harmful: a client blocked for seconds may time out
 * locally and resubmit while the original is still in flight. The idempotency key makes that safe, but a
 * fast rejection the client can back off from is better than a slow success it has stopped waiting for.
 */
@Configuration(proxyBeanMethods = false)
public class ResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(ResilienceConfig.class);

    /**
     * The name {@code @RateLimiter} on the mutation refers to. Kept as a constant so the annotation and
     * this configuration cannot drift: a typo in the annotation would silently resolve to a default
     * limiter with entirely different limits rather than failing.
     */
    public static final String POST_TRANSACTION_LIMITER = "postTransaction";

    /**
     * Permits per {@link #REFRESH_PERIOD}, per instance.
     *
     * <p>Chosen against the engine's actual bottleneck rather than picked round. The JDBC pool holds 32
     * connections and a posting occupies one for the duration of its transaction; at a few milliseconds
     * per posting the pool sustains well over a thousand per second, so 200 leaves substantial headroom
     * for the query path and the outbox relay, which share that pool. It is a backstop against a spike,
     * not a throttle on normal traffic.
     */
    private static final int PERMITS_PER_PERIOD = 200;

    /**
     * One second, so the limit reads as "200/sec".
     *
     * <p>Resilience4j refills the whole allowance at each period boundary rather than smoothing it, so a
     * long period would permit a large burst at the start and starve the remainder. A one-second period
     * keeps the burst bounded at the permit count.
     */
    private static final Duration REFRESH_PERIOD = Duration.ofSeconds(1);

    /** Zero: refuse immediately rather than parking the caller. See the class notes. */
    private static final Duration TIMEOUT_DURATION = Duration.ZERO;

    /**
     * The rate limiter registry.
     *
     * <p>Configured in code rather than under {@code resilience4j.ratelimiter.*} in
     * {@code application.yml} for one reason: the numbers above are only defensible alongside the
     * reasoning that ties them to the connection pool. As YAML they would be three unexplained values
     * that the next person tunes by guesswork. Per-environment overrides remain possible — Spring Boot's
     * Resilience4j properties still apply on top of this registry.
     */
    @Bean
    public RateLimiterRegistry rateLimiterRegistry() {
        RateLimiterConfig postTransactionConfig = RateLimiterConfig.custom()
                .limitForPeriod(PERMITS_PER_PERIOD)
                .limitRefreshPeriod(REFRESH_PERIOD)
                .timeoutDuration(TIMEOUT_DURATION)
                // Do not fill in a stack trace on the thrown RequestNotPermitted. It is caught and
                // mapped to a GraphQL error a few frames away and the trace is never read, so
                // capturing one is pure cost — paid once per rejection, at exactly the moment the
                // system is already under more load than it can take.
                .writableStackTraceEnabled(false)
                .build();

        RateLimiterRegistry registry = RateLimiterRegistry.of(postTransactionConfig);
        // Instantiate eagerly so the limiter and its metrics exist from startup. Left lazy, the gauges
        // would not appear until the first mutation, and a dashboard would show nothing during exactly
        // the quiet period an operator uses to confirm the deploy is healthy.
        registry.rateLimiter(POST_TRANSACTION_LIMITER, postTransactionConfig);
        return registry;
    }

    /**
     * Publishes the limiter's metrics to Micrometer.
     *
     * <p>Gives {@code resilience4j.ratelimiter.available.permissions} and
     * {@code resilience4j.ratelimiter.waiting_threads} — headroom, and whether anyone is queueing
     * (always zero here, since the timeout is zero).
     *
     * <p>Note what these do <em>not</em> give: a count of rejections. Resilience4j publishes no
     * {@code calls} counter for a rate limiter, and a shed request never reaches the engine, so it is
     * absent from {@code apex.ledger.posting.result} too. Shed load would be invisible except as a dip
     * in a gauge. {@code LedgerExceptionResolver} therefore counts it explicitly as
     * {@code apex.ledger.api.rate.limited}, and that is the meter to alert on.
     */
    @Bean
    public TaggedRateLimiterMetrics rateLimiterMetrics(RateLimiterRegistry rateLimiterRegistry,
                                                      MeterRegistry meterRegistry) {
        TaggedRateLimiterMetrics metrics =
                TaggedRateLimiterMetrics.ofRateLimiterRegistry(rateLimiterRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    /**
     * Logs the effective limit at startup, and fails if the named limiter is missing.
     *
     * <p>The failure case is the point. {@code @RateLimiter(name = "...")} resolves lazily, so a name
     * that does not match anything here would quietly fall back to Resilience4j's defaults — a limiter
     * with 50 permits and a 5-second timeout, which both throttles legitimate traffic and parks callers.
     * Asserting the wiring at startup turns that silent misconfiguration into a boot failure.
     */
    @Bean
    InitializingBean rateLimiterWiringCheck(RateLimiterRegistry rateLimiterRegistry) {
        return () -> {
            RateLimiterConfig config = rateLimiterRegistry
                    .find(POST_TRANSACTION_LIMITER)
                    .orElseThrow(() -> new IllegalStateException(
                            "rate limiter '%s' is not registered, so @RateLimiter on the posting "
                                    + "mutation would silently use Resilience4j's defaults"
                                    .formatted(POST_TRANSACTION_LIMITER)))
                    .getRateLimiterConfig();

            log.info("rate limiter '{}' active: {} permits per {} per instance, {} timeout "
                            + "(immediate rejection)",
                    POST_TRANSACTION_LIMITER,
                    config.getLimitForPeriod(),
                    config.getLimitRefreshPeriod(),
                    config.getTimeoutDuration());
        };
    }
}
