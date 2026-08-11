package com.apex.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Entry point for the ApexLedger engine.
 *
 * <p>Runs on Java 21 virtual threads: {@code spring.threads.virtual.enabled=true} in
 * {@code application.yml} makes Tomcat dispatch every request onto its own virtual thread and gives
 * the Kafka listener containers virtual-thread executors. Nothing in this codebase should therefore
 * pool threads by hand, cache state in a {@link ThreadLocal} keyed to a pooled worker, or treat
 * thread creation as expensive.
 *
 * <p>{@code @ConfigurationPropertiesScan} binds the {@code apex.*} configuration tree without
 * requiring each holder to be a component.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ApexLedgerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApexLedgerApplication.class, args);
    }
}
