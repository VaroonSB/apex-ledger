package com.apex.ledger.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables {@code @Scheduled}, which the outbox relay depends on.
 *
 * <p>With {@code spring.threads.virtual.enabled=true} Spring Boot backs the scheduler with a
 * virtual-thread {@code SimpleAsyncTaskScheduler}, so a relay tick that blocks on Kafka or JDBC does not
 * occupy a scarce platform thread — and, unlike the default single-threaded pool, a slow tick cannot
 * delay unrelated scheduled work.
 *
 * <p>Every instance of the application runs its own relay. That is safe rather than merely tolerated:
 * the relay claims rows with {@code FOR UPDATE SKIP LOCKED}, so concurrent workers take disjoint sets
 * and no leader election is needed.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class SchedulingConfig {
}
