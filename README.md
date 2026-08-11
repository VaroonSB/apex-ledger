# ApexLedger

A production-grade, highly concurrent, multi-currency **double-entry ledger engine**.

Spring Boot 3.5.16 · Java 21 (virtual threads) · Spring GraphQL · PostgreSQL 16 · Redis 7 · Apache
Kafka 4 (KRaft)

> **Status: Phase 1 — architecture and scaffolding.** The build, the infrastructure plane and the
> runtime configuration are complete and verified. The domain model, GraphQL schema, persistence
> layer and concurrency-control implementations arrive in Phase 2.

---

## Quick start

```bash
# 1. Bring up PostgreSQL, Redis and Kafka, then wait for all three to be healthy
docker compose up -d
docker compose ps                # every STATUS must read "healthy", not just "running"

# 2. Build
mvn clean verify

# 3. Run
mvn spring-boot:run
```

Verify the service is up:

```bash
curl -s localhost:8080/actuator/health/readiness

curl -s localhost:8080/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ ping }"}'
# => {"data":{"ping":"pong"}}
```

GraphiQL is served at <http://localhost:8080/graphiql>.

To have the app manage the compose stack itself, start it with
`--spring.docker.compose.enabled=true`.

## Ports

| Service | Host port | Notes |
|---|---|---|
| ApexLedger | 8080 | `/graphql`, `/graphiql`, `/actuator` |
| PostgreSQL | 5432 | db `apex_ledger`, user `apex` |
| Redis | 6379 | no auth locally |
| Kafka | 9092 | `EXTERNAL` listener; containers use `kafka:19092` |

Credentials in `docker-compose.yml` and the defaults in `application.yml` are **local development
values only**. Every one is overridable by environment variable (`APEX_DB_PASSWORD`,
`APEX_REDIS_PASSWORD`, `APEX_KAFKA_BOOTSTRAP_SERVERS`, …) and must be supplied from a secret store
in any deployed environment.

---

## Architecture

Ports and adapters, with a framework-free domain core. See
[`docs/PROJECT_STRUCTURE.md`](docs/PROJECT_STRUCTURE.md) for the full tree and the rationale.

```
     GraphQL (api/)
          │
          ▼
   application/  ── ports ──▶  infrastructure/  ──▶  PostgreSQL · Redis · Kafka
          │
          ▼
      domain/   ← no Spring, no JPA, no Kafka
```

### The three properties this design has to deliver

**Immutability.** The journal is append-only. There are no `UPDATE` or `DELETE` paths against
journal tables; corrections are new compensating entries. Schema is owned by Flyway with
`hibernate.ddl-auto=validate`, so the ORM can never mutate live DDL and a drifted entity mapping
fails the boot instead of the audit.

**Concurrency safety.** Double-spend is prevented by PostgreSQL **transactional advisory locks**
taken on account identifiers inside the posting transaction. The database releases them on commit or
rollback — including when the JVM dies mid-transfer — so a crashed node cannot leave an account
permanently locked. Client-level retries are collapsed by a Redis `SET NX` idempotency key, backed
by a unique constraint on the journal that is the actual authority. Redis runs
`maxmemory-policy noeviction` for exactly this reason: an evicted idempotency key would turn a retry
into a second posting.

**Scalability.** `spring.threads.virtual.enabled=true` puts every request and Kafka listener on a
virtual thread, so the HTTP tier stops being the bottleneck. The interesting consequence is that the
**bounded Hikari pool becomes the admission-control point**, and that is deliberate: unbounded
virtual threads queueing on a 32-connection pool is precisely the backpressure a ledger wants,
because it caps how many transactions can hold row locks at once. Removing that bound would let tens
of thousands of virtual threads pile onto the same contended account rows and convert a throughput
problem into a deadlock storm.

One caveat tracked in the config comments: on Java 21 a blocking `synchronized` block pins its
carrier thread (JEP 491 fixed this only in Java 24), and pgjdbc still synchronizes internally. Keep
the carrier pool at least as large as the JDBC pool so pinning cannot starve the scheduler.

### Money

Amounts are `BigDecimal` end to end, never `double`. `application.yml` enforces this at the
serialization boundary in both directions:
`USE_BIG_DECIMAL_FOR_FLOATS` on the way in and `WRITE_BIGDECIMAL_AS_PLAIN` on the way out, so no
amount is ever parsed through a binary float or emitted in scientific notation. No implicit
cross-currency conversion happens during a posting; each entry must balance to zero **per
currency**.

---

## Testing

| Suite | Pattern | Runner | Needs Docker |
|---|---|---|---|
| Unit | `*Test`, `*Tests` | Surefire (`mvn test`) | no |
| Integration | `*IT` | Failsafe (`mvn verify`) | yes |

Integration tests use Testcontainers (PostgreSQL and Kafka modules) with Spring Boot's
`@ServiceConnection`, so connection details are derived from live containers and never hard-coded.
`mvn verify -DskipITs` runs the unit suite alone.

The concurrency suite (`src/test/java/com/apex/ledger/concurrency/`) is the load-bearing one: it
fires N concurrent transfers at a single account and asserts that exactly one succeeds, that no
balance goes negative, and that the journal still sums to zero.

---

## Configuration

All runtime configuration lives in a single file, `src/main/resources/application.yml`, with inline
comments explaining every non-obvious value. Notable choices:

- `synchronous_commit=on` and `fsync=on` in PostgreSQL — never relaxed for benchmarks
- `wal_level=logical` so an outbox reader can tail committed entries without polling
- `track_commit_timestamp=on` for server-authoritative entry ordering
- Kafka producer `acks=all` with `enable.idempotence=true` and
  `max.in.flight.requests.per.connection=5` (the ceiling that still preserves ordering)
- Kafka consumer `enable-auto-commit=false` with `ack-mode=manual_immediate`
- `auto.create.topics.enable=false` on the broker, so a typo'd topic name fails loudly
- `log.retention.hours=-1` — the event log is an audit record and is never aged out

---

## Phase 1 verification

What was checked, rather than assumed:

- `mvn clean verify` → `BUILD SUCCESS` on Java 21.0.10 / Maven 3.9.11
- `docker compose config` → schema valid; images, ports, healthchecks and volumes resolve
- All 210 leaf keys in `application.yml` validated against the
  `spring-configuration-metadata.json` shipped in the 212 resolved jars
- Every enum-valued property validated against its actual enum constants via `javap`
- Image tags confirmed to exist against the Docker Hub registry API
- `/var/lib/kafka/data` confirmed present and `appuser`-owned in `apache/kafka:4.1.2`, so the named
  volume inherits writable ownership instead of landing root-owned

## Phase 2 (not yet implemented)

Domain model and invariants · Flyway migrations for the journal schema · GraphQL domain schema,
custom scalars and error mapping · advisory-lock and idempotency adapters · transactional outbox →
Kafka · Redis balance projections · the Testcontainers and double-spend suites.
