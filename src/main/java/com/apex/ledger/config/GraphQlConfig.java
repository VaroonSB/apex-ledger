package com.apex.ledger.config;

import com.apex.ledger.api.graphql.scalar.LedgerScalars;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;

/**
 * Registers the schema's custom scalars.
 *
 * <p>Required, not optional: graphql-java fails schema construction for a {@code scalar} declaration
 * with no runtime implementation, so the application will not start if these are missing. That is the
 * desired behaviour — a silently unwired money scalar would be far worse than a failed boot.
 */
@Configuration(proxyBeanMethods = false)
public class GraphQlConfig {

    @Bean
    public RuntimeWiringConfigurer ledgerScalarsConfigurer() {
        return wiringBuilder -> wiringBuilder
                .scalar(LedgerScalars.decimal())
                .scalar(LedgerScalars.dateTime());
    }
}
