package com.apex.ledger.api.graphql.controller;

import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

/**
 * Resolves {@code Query.ping}.
 *
 * <p>Exists so the scaffold is verifiable end to end: it proves the GraphQL transport, schema
 * loading and annotated-controller wiring are all functional, and it keeps schema inspection clean
 * by leaving no unmapped field on the root {@code Query} type.
 *
 * <p>Deliberately touches no infrastructure. Dependency health belongs to the Actuator readiness
 * group, which aggregates the PostgreSQL and Redis indicators.
 */
@Controller
public class PingController {

    @QueryMapping
    public String ping() {
        return "pong";
    }
}
