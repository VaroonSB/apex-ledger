# ApexLedger — Project Structure

Standard Maven layout for the `com.apex.ledger` namespace, organised as **ports and adapters**
(hexagonal). The rule that drives the layout: **dependencies point inward.** `domain` knows nothing
about Spring, JPA, Kafka or GraphQL; `application` depends only on `domain` and on interfaces it
declares itself; `infrastructure` and `api` are replaceable adapters at the edges.

This matters for a ledger specifically. The double-entry invariants (every entry balances to zero
per currency, no account is debited below its permitted floor, entries are append-only) must be
enforceable and testable without a database, a broker or a running Spring context. Anything that
can only be verified with infrastructure attached will eventually be verified by nobody.

```
apex-ledger/
├── pom.xml                              # Maven build — Spring Boot 3.5.16 parent, Java 21
├── docker-compose.yml                   # PostgreSQL 16 · Redis 7 · Kafka 4 (KRaft)
├── .gitignore
├── README.md
├── docs/
│   └── PROJECT_STRUCTURE.md             # this file
└── src/
    ├── main/
    │   ├── java/com/apex/ledger/
    │   │   ├── ApexLedgerApplication.java        # @SpringBootApplication entry point
    │   │   │
    │   │   ├── config/                           # Spring @Configuration only — no business logic
    │   │   │                                     #   RedisConfig (serializers, lease scripts)
    │   │   │                                     #   KafkaConfig (NewTopic beans, DLQ recoverer)
    │   │   │                                     #   JacksonConfig, GraphQlWiringConfig
    │   │   │                                     #   VirtualThreadConfig (executors, carrier sizing)
    │   │   │
    │   │   ├── domain/                           # ── PURE CORE — no framework imports ──
    │   │   │   ├── model/                        #   Account, JournalEntry, Posting,
    │   │   │   │                                 #   Money, CurrencyCode, AccountId,
    │   │   │   │                                 #   Direction (DEBIT/CREDIT)
    │   │   │   │                                 #   Java records + value objects; immutable.
    │   │   │   │                                 #   Balance invariants enforced in constructors.
    │   │   │   ├── event/                        #   JournalEntryPosted, BalanceProjected —
    │   │   │   │                                 #   the published contract; versioned, additive.
    │   │   │   └── exception/                    #   InsufficientFundsException,
    │   │   │                                     #   UnbalancedEntryException,
    │   │   │                                     #   CurrencyMismatchException
    │   │   │
    │   │   ├── application/                      # ── USE CASES / ORCHESTRATION ──
    │   │   │   ├── port/
    │   │   │   │   ├── in/                       #   PostTransferUseCase, QueryBalanceUseCase
    │   │   │   │   └── out/                      #   JournalRepository, BalanceCache,
    │   │   │   │                                 #   EventPublisher, AccountLock,
    │   │   │   │                                 #   IdempotencyStore
    │   │   │   │                                 #   Interfaces OWNED BY THIS LAYER and
    │   │   │   │                                 #   implemented in infrastructure/ —
    │   │   │   │                                 #   this is what inverts the dependency.
    │   │   │   └── service/                      #   @Transactional use-case implementations.
    │   │   │                                     #   The lock → validate → append → publish
    │   │   │                                     #   ordering that defeats double-spend lives here.
    │   │   │
    │   │   ├── infrastructure/                   # ── DRIVEN ADAPTERS ──
    │   │   │   ├── persistence/
    │   │   │   │   ├── entity/                   #   JPA @Entity — insert-only, @Version where
    │   │   │   │   │                             #   optimistic checks apply. Separate from
    │   │   │   │   │                             #   domain/model on purpose: the ORM must not
    │   │   │   │   │                             #   dictate the domain's shape.
    │   │   │   │   ├── repository/               #   Spring Data JPA + port adapters.
    │   │   │   │   │                             #   Pessimistic locking (SELECT … FOR UPDATE)
    │   │   │   │   │                             #   and advisory-lock queries.
    │   │   │   │   ├── mapper/                   #   entity ⇄ domain translation
    │   │   │   │   └── converter/                #   JPA AttributeConverters for the value
    │   │   │   │                                 #   objects: CurrencyCode, IdempotencyKey,
    │   │   │   │                                 #   RequestFingerprint
    │   │   │   ├── messaging/
    │   │   │   │   ├── producer/                 #   transactional outbox → Kafka
    │   │   │   │   ├── consumer/                 #   projection builders, DLQ handling
    │   │   │   │   └── serde/                    #   JsonSerializer/Deserializer config
    │   │   │   ├── cache/                        #   Redis balance snapshots, read-through
    │   │   │   ├── concurrency/                  #   AccountLock + IdempotencyStore impls.
    │   │   │   │                                 #   PostgreSQL transactional advisory locks
    │   │   │   │                                 #   (authoritative) and Redis leases
    │   │   │   │                                 #   (coarse-grained guard).
    │   │   │   └── observability/                #   Micrometer meters, tracing decorators
    │   │   │
    │   │   └── api/                              # ── DRIVING ADAPTER ──
    │   │       └── graphql/
    │   │           ├── controller/               #   @QueryMapping / @MutationMapping
    │   │           ├── dto/                      #   GraphQL input/payload records
    │   │           ├── scalar/                   #   Decimal, DateTime, UUID, CurrencyCode
    │   │           └── error/                    #   DataFetcherExceptionResolver —
    │   │                                         #   maps domain exceptions to typed
    │   │                                         #   GraphQL errors, leaks no internals
    │   │
    │   └── resources/
    │       ├── application.yml                   # single source of runtime configuration
    │       ├── db/migration/                     # Flyway V<n>__<desc>.sql — immutable once applied
    │       └── graphql/
    │           └── schema.graphqls               # root Query; domain schema added in Phase 3
    │
    └── test/
        ├── java/com/apex/ledger/
        │   ├── support/                          # AbstractIntegrationTest — @ServiceConnection
        │   │                                     # container singletons (Postgres, Redis, Kafka),
        │   │                                     # reused across the suite
        │   ├── domain/                           # pure unit tests — no Spring, milliseconds
        │   ├── api/graphql/                      # @GraphQlTest slices with GraphQlTester
        │   ├── integration/                      # *IT — full stack on Testcontainers
        │   └── concurrency/                      # *IT — the double-spend suite: N concurrent
        │                                         # transfers against one account, asserting
        │                                         # exactly one succeeds and the journal still
        │                                         # sums to zero
        └── resources/
            └── application-test.yml              # test overrides
```

## Test naming and execution

Surefire runs `*Test` / `*Tests` (fast, no Docker). Failsafe runs `*IT` during `mvn verify` and is
the only phase that needs a container runtime. Keeping them split means a developer without Docker
can still run the domain suite, and CI can fan the two out to different runners.

## Where the concurrency guarantees live

| Concern | Owner | Mechanism |
|---|---|---|
| Double-spend / concurrent debit | `db/migration` — `trg_journal_entries_apply_balance` + `ck_accounts_minimum_balance` | The balance trigger's UPDATE row-locks the account, serialising concurrent postings; the CHECK then rejects an overdraft. Phase 3 adds advisory locks for deterministic multi-account lock **ordering**, not for the guarantee itself |
| Duplicate client submission | `infrastructure/concurrency` (`DatabaseIdempotencyGuard`) | `uq_transactions_idempotency_key`. The key is on the `transactions` row, so the reservation is atomic with the postings. Redis `SET NX` is a Phase 3 fast path, never the authority |
| Lost events | `infrastructure/messaging/producer` | transactional outbox written in the same DB transaction as the entry, then relayed to Kafka |
| Immutability | `infrastructure/persistence` | insert-only entities, no `UPDATE`/`DELETE` grants on journal tables, `ddl-auto=validate` |
| Admission control | `application.yml` (Hikari pool) | bounded pool caps concurrent row-lock holders even though virtual threads are unbounded |

The Redis lease is a fast-path guard, not the source of truth. Redis is configured
`maxmemory-policy noeviction` precisely because an evicted idempotency key would otherwise convert a
client retry into a second posting — but the database constraint is what actually makes that
impossible.

As of Phase 2 the ledger tables, entities, repositories and idempotency guard exist and are verified
against a real PostgreSQL 16; `domain/model` holds the value objects and enums, while the JPA
entities live in `infrastructure/persistence/entity` per the inward-dependency rule above. The
`domain/event`, `application/port/in`, `application/service`, `messaging`, `cache` and `api/graphql`
packages are still empty and are filled in Phase 3.
