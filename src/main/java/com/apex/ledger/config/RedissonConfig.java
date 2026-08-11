package com.apex.ledger.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.EqualJitterDelay;
import org.redisson.config.SingleServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Builds the {@link RedissonClient} used for distributed account locking.
 *
 * <h2>Why the client is built here rather than by a starter</h2>
 *
 * <p>{@code redisson-spring-boot-starter} ships its own {@code spring-data-redis} and
 * {@code spring-boot} versions and would compete with the ones Spring Boot manages. Building the
 * client explicitly keeps exactly one Redis client stack on the classpath and makes every timeout
 * visible in code rather than spread across a second properties namespace.
 *
 * <h2>One source of truth for connection details</h2>
 *
 * <p>The address comes from {@link RedisConnectionDetails}, the same abstraction the Lettuce
 * auto-configuration consumes — not from {@code @Value("${spring.data.redis.host}")}. That matters:
 * {@code RedisConnectionDetails} is what Spring Boot's {@code @ServiceConnection} and Docker Compose
 * support contribute to. Reading the raw properties instead would leave Redisson pointing at
 * {@code localhost} while Lettuce talked to a Testcontainers-assigned port — a split-brain that only
 * shows up as locks that appear to work but protect nothing.
 *
 * <h2>Virtual threads</h2>
 *
 * <p>Redisson is Netty-based: commands are issued asynchronously on its own event-loop threads, and a
 * caller waiting for a lock blocks on a {@code CompletableFuture} / {@code Semaphore}. Both park the
 * carrier-friendly way, so a virtual thread waiting for a lock releases its carrier rather than
 * pinning it.
 *
 * <p>The configuration below deliberately keeps it that way. {@code lockWatchdogTimeout} is left at
 * its default but is <strong>never engaged</strong>, because every lock in this application is taken
 * with an explicit lease time; the watchdog only runs for leases of {@code -1}. That avoids a
 * background renewal task per held lock, and — more importantly — avoids renewal state keyed by
 * {@code Thread#threadId()}, which would accumulate an entry per virtual thread.
 *
 * <p>Pinning is a property of {@code synchronized} blocks that block, not of blocking as such. If a
 * carrier shortage is ever suspected in production, confirm it with the JFR event
 * {@code jdk.VirtualThreadPinned} rather than by inference.
 */
@Configuration(proxyBeanMethods = false)
public class RedissonConfig {

    private static final Logger log = LoggerFactory.getLogger(RedissonConfig.class);

    /**
     * Locking traffic is small and bursty: a few short-lived commands per posting. Sized well below
     * the Lettuce pool because Redisson multiplexes commands over these connections rather than
     * dedicating one per caller, so a large pool buys nothing and costs file descriptors.
     */
    private static final int CONNECTION_POOL_SIZE = 24;
    private static final int MINIMUM_IDLE_CONNECTIONS = 6;

    /**
     * Lock acquisition uses pub/sub: a waiter subscribes to the lock's channel and is woken when the
     * holder releases. These connections are separate from the command pool, and starving them shows
     * up as waiters that only wake on timeout instead of promptly.
     */
    private static final int SUBSCRIPTION_POOL_SIZE = 8;
    private static final int SUBSCRIPTION_MINIMUM_IDLE = 2;

    /** Bounded so a Redis stall surfaces as a failed posting rather than an indefinite hang. */
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(3);
    private static final int RETRY_ATTEMPTS = 3;

    /**
     * Retry backoff with jitter, replacing the deprecated fixed {@code setRetryInterval}.
     *
     * <p>Jitter matters here rather than being a refinement: when a Redis blip fails many in-flight
     * lock commands at once, a constant interval makes every application node retry in lockstep and
     * hammer the recovering server in synchronised waves. Equal jitter spreads them.
     *
     * <p>The ceiling is kept well inside {@code COMMAND_TIMEOUT} so retries cannot silently stretch a
     * command past the timeout the caller is relying on.
     */
    private static final Duration RETRY_BASE_DELAY = Duration.ofMillis(100);
    private static final Duration RETRY_MAX_DELAY = Duration.ofMillis(600);

    /** Detects a silently dropped connection well inside the shortest lock lease. */
    private static final Duration PING_CONNECTION_INTERVAL = Duration.ofSeconds(10);

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(RedisConnectionDetails connectionDetails) {
        RedisConnectionDetails.Standalone standalone = connectionDetails.getStandalone();
        if (standalone == null) {
            throw new IllegalStateException(
                    "ApexLedger requires a standalone Redis endpoint for distributed locking; "
                            + "sentinel and cluster topologies are not configured in this phase");
        }

        boolean sslEnabled = standalone.getSslBundle() != null;
        String address = "%s://%s:%d".formatted(
                sslEnabled ? "rediss" : "redis", standalone.getHost(), standalone.getPort());

        Config config = new Config();

        // StringCodec keeps every value Redisson writes human-readable in redis-cli, and — more
        // importantly — avoids Redisson's default Kryo/JDK-flavoured codec, which would deserialize
        // arbitrary types out of Redis. Lock values are short strings, so nothing is lost.
        config.setCodec(StringCodec.INSTANCE);

        // Redisson would otherwise create its own thread pools sized from the CPU count. The
        // application already runs request work on virtual threads; these threads exist purely to
        // drive Netty and should stay small and fixed.
        config.setNettyThreads(8);
        config.setThreads(8);

        // Redisson's own reference-counted executor is not needed: the client is a singleton owned by
        // the Spring context and shut down with it.
        config.setUseScriptCache(true);

        SingleServerConfig server = config.useSingleServer()
                .setAddress(address)
                .setDatabase(standalone.getDatabase())
                .setClientName("apex-ledger-lock")
                .setConnectionPoolSize(CONNECTION_POOL_SIZE)
                .setConnectionMinimumIdleSize(MINIMUM_IDLE_CONNECTIONS)
                .setSubscriptionConnectionPoolSize(SUBSCRIPTION_POOL_SIZE)
                .setSubscriptionConnectionMinimumIdleSize(SUBSCRIPTION_MINIMUM_IDLE)
                .setTimeout((int) COMMAND_TIMEOUT.toMillis())
                .setConnectTimeout((int) CONNECT_TIMEOUT.toMillis())
                .setRetryAttempts(RETRY_ATTEMPTS)
                .setRetryDelay(new EqualJitterDelay(RETRY_BASE_DELAY, RETRY_MAX_DELAY))
                .setPingConnectionInterval((int) PING_CONNECTION_INTERVAL.toMillis())
                .setKeepAlive(true);

        String username = connectionDetails.getUsername();
        if (username != null && !username.isBlank()) {
            server.setUsername(username);
        }
        String password = connectionDetails.getPassword();
        if (password != null && !password.isBlank()) {
            server.setPassword(password);
        }

        log.info("Redisson lock client configured for {} (database {}, ssl {})",
                address, standalone.getDatabase(), sslEnabled);

        return Redisson.create(config);
    }

    /**
     * Fails startup if the lock lease could expire while a posting transaction is still running.
     *
     * <p>This is not a style check. If {@code leaseTime <= transactionTimeout}, a slow posting can
     * outlive its own lock: Redis hands the lock to another node while the first is still inside its
     * transaction, and the mutual exclusion the engine believes it has silently does not exist. Redis
     * cannot revoke a lease it has already granted, so there is no runtime recovery — the only defence
     * is refusing to start.
     *
     * <p>The ledger still holds, because PostgreSQL constraints are the authority for correctness.
     * What is lost is the coordination and cache coherence the lock is there to provide, which is
     * exactly the kind of degradation that is invisible until reconciliation.
     */
    @Bean
    InitializingBean lockLeaseTimeoutValidator(
            ApexLedgerProperties properties,
            @Value("${spring.transaction.default-timeout}") Duration transactionTimeout) {
        return () -> {
            Duration leaseTime = properties.locking().leaseTime();
            if (leaseTime.compareTo(transactionTimeout) <= 0) {
                throw new IllegalStateException(
                        ("apex.ledger.locking.lease-time (%s) must exceed "
                                + "spring.transaction.default-timeout (%s), otherwise a lock can "
                                + "expire while the transaction it guards is still running and two "
                                + "nodes will post concurrently")
                                .formatted(leaseTime, transactionTimeout));
            }
            log.info("Lock lease {} safely exceeds transaction timeout {}",
                    leaseTime, transactionTimeout);
        };
    }
}
