# ApexLedger

A production-grade, highly concurrent, multi-currency **double-entry ledger engine**.

Spring Boot 3.5.16 · Java 21 (virtual threads) · Spring GraphQL · PostgreSQL 16 · Redis 7 · Apache
Kafka 4 (KRaft)

> **Status: complete (Phase 5).** Observability, resilience and a container-backed concurrency suite
> are in place. The full path is verified end to end: GraphQL mutation → engine → outbox (same
> commit) → relay → Kafka → audit consumer, under a 50-way virtual-thread stampede.

---

**Setting this up locally?** See [`docs/LOCAL_SETUP.md`](docs/LOCAL_SETUP.md) for Mac-specific steps,
a post-setup verification checklist, and what must change before this goes anywhere real.

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

**Concurrency safety.** Double-spend is prevented **in the storage engine**, not in application code.
Every journal entry insert fires a trigger that folds the amount into its account's running totals;
that UPDATE takes a row lock, so concurrent postings against one account serialise, and each is then
re-checked against the `ck_accounts_minimum_balance` constraint using the balance its predecessor
left behind. Verified: 8 concurrent withdrawals against a balance of 100 yield exactly one commit,
with the balance floored at 0 and no application locking involved at all.

Duplicate submissions are collapsed by the `uq_transactions_idempotency_key` unique constraint. The
key lives on the `transactions` row, so claiming it and writing the postings are one atomic act —
there is no separate reservation that could outlive a failed posting and block a legitimate retry.
Phase 3 adds PostgreSQL transactional advisory locks on top, not for correctness but to impose a
deterministic multi-account lock order so two opposing transfers cannot deadlock, plus a Redis
`SET NX` fast path. Redis runs `maxmemory-policy noeviction` because an evicted idempotency key would
turn a retry into a second posting — though the database constraint is what makes that impossible.

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

## Where the invariants are enforced

Phase 2 pushed every ledger invariant into the database, so no application bug can violate one.

| Invariant | Enforced by | Verified |
|---|---|---|
| Journal is append-only | `trg_journal_entries_append_only`, plus `@Immutable`, `updatable=false`, `final` class, no setters, no repository delete | UPDATE, DELETE, zero-row UPDATE and TRUNCATE all rejected |
| Every transaction balances per currency | `trg_journal_entries_balanced` (deferred to COMMIT) | 100 vs 60 rejected; single-sided entry rejected |
| Balance projection cannot drift | `trg_journal_entries_apply_balance` folds each entry into its account in the same statement | reconciliation query returns zero rows |
| No overdraft | `ck_accounts_minimum_balance`, evaluated inside the balance trigger's UPDATE | 8 concurrent withdrawals of a 100 balance → exactly 1 committed, balance floored at 0 |
| Entry currency = account currency | composite FK `(account_id, currency)` → `accounts (id, currency)` | EUR entry on a USD account rejected |
| No duplicate submission | `uq_transactions_idempotency_key` | 8 concurrent submissions of one key → exactly 1 transaction |
| No posting to a frozen account | `apex_apply_entry_to_account_balance()` | FROZEN account rejected |
| At most one reversal per transaction | `uq_transactions_reverses` | second reversal rejected |
| Amounts never finer than the currency | `Money` / `CurrencyCode` | `1.005 USD` and `500.5 JPY` rejected; `XAU` refused as non-transactable |

### A consequence worth knowing

Because the append-only triggers reject DELETE and TRUNCATE on `journal_entries` and `transactions`,
the usual "truncate between tests" strategy is impossible against those tables. Tests get isolation
from a fresh Testcontainers database per run, and create their own accounts and unique idempotency
keys. This is the immutability guarantee working as designed, not a gap.

## The posting pipeline

`LedgerEngineService.post` runs eight steps, and the order is the design:

```
1. validate double-entry            in memory — reject nonsense before spending a lock or a connection
2. fingerprint the request          SHA-256 over a canonical, leg-sorted form
3. idempotency fast path            a replay returns the original outcome without locking
4. ACQUIRE DISTRIBUTED LOCK         sorted account set, explicit wait (2s) + lease (15s)
5. @Transactional persist           transaction + journal entries + outbox event, one commit
6. read committed balances          authoritative, post-commit
7. write-through the balance cache  still holding the lock
8. RELEASE LOCK                     try-with-resources, same thread that acquired
```

**Step 4 comes before step 5** deliberately. Opening the transaction first and *then* waiting on Redis
would hold a pooled JDBC connection for the whole wait; under contention on one hot account that
exhausts the 32-connection pool and takes down every account. Locks are always acquired outside the
transaction.

**Two locks, two jobs.** The Redisson lock is *coordination*, not correctness. A lease can expire while
its holder is still working, after which another node legitimately acquires it — Redis cannot revoke a
lease already granted, and there is no fencing token. What it buys is that N concurrent requests for one
account do not all reach PostgreSQL, take row locks, do the work and then get all-but-one rejected;
plus bounded, typed failure; plus cache coherence. Correctness stays where Phase 2 put it: the balance
trigger's row lock and `ck_accounts_minimum_balance`.

`RedissonConfig` refuses to start if `lease-time <= spring.transaction.default-timeout`, because a lease
that can expire mid-transaction silently voids the mutual exclusion the engine believes it has.

### Verified against real PostgreSQL 16 + Redis 7

| Property | Result |
|---|---|
| Contended withdrawals serialise rather than fail | 8 concurrent withdrawals of a 100 balance → 1 success, 7 `InsufficientFunds`, **0 lock timeouts** |
| Opposing transfers do not deadlock | 24 concurrent alternating A→B / B→A transfers → all succeed; total lock ordering by account id |
| A lock held elsewhere times out cleanly | bounded by the 2s wait, typed `AccountLockTimeoutException`, retryable |
| Concurrent submissions of one key | 8 racers → exactly 1 posting |
| Leg order does not affect the fingerprint | reversed legs replay instead of conflicting |
| Atomicity of the triple | transaction + 2 entries + 1 `PENDING` outbox row, or nothing |
| Cache fence discards out-of-order writes | a stale-fence write is rejected; a newer one applies |
| Lease expiry during posting | 0 occurrences |

## The API and event pipeline

```
GraphQL mutation ──▶ LedgerEngineService ──▶ one commit: transaction + entries + outbox row
                                                             │
                                    OutboxRelayService (@Scheduled, SKIP LOCKED)
                                                             ▼
                                              Kafka  apex.ledger.journal-entries.v1
                                                             │
                                                   AsyncAuditConsumer (@KafkaListener)
```

**The payload is published verbatim.** The relay sends the exact bytes committed with the ledger
change — it never deserialises and re-serialises. Re-encoding would make the published record depend
on this service's current Jackson configuration rather than on what was committed, so an event
replayed from the outbox months later could differ from the one originally published.

**Delivery is at-least-once, and that is inherent.** Claiming a database row and acknowledging a Kafka
write cannot be made atomic, so the relay can publish and then fail before marking the row. Consumers
deduplicate on the `apex-event-id` header, which is stable across every redelivery.

### Money on the wire

`Decimal` is serialised as a **plain-notation string**, never a JSON number. A JSON number would be
parsed by a JavaScript client as an IEEE double, silently altering a 20-digit balance before the
application ever saw it. On input the scalar accepts a string or an exact number and **refuses a
`Double` outright** — by the time an amount is a `double` its precision is already gone.

### Cursor pagination

`getTransactionHistory` uses keyset cursors encoding `(createdAt, id)`, not offsets. On an append-only
journal an offset shifts every time a posting lands ahead of the window, so an offset-paged client
silently skips or repeats rows while it reads. The id is part of the key because `created_at` is not
unique — the postings of one transaction share a timestamp.

### Verified end to end

| Property | Result |
|---|---|
| Mutation → outbox → Kafka → consumer | 27 events relayed, 27 audited, 0 DLQ, 0 abandoned |
| Payload published byte-for-byte | record value equals the committed `payload` exactly |
| Producer durability | `acks=all`, `enable.idempotence=true` confirmed at runtime |
| `Decimal` on the wire | `"100.00"` as a string; scale 2 survives the jsonb round trip |
| `Decimal` refuses a float | `parseValue(100.005d)` throws |
| Error mapping | `IDEMPOTENCY_KEY_REUSED`, `INSUFFICIENT_FUNDS`, `INVALID_INPUT`, `INVALID_CURSOR` with `retryable` flags |
| Idempotent replay through GraphQL | `replayed: true`, same transaction id, no second posting |
| Cursor pagination | 7 entries at 3/page → 3 pages, each entry seen exactly once |
| Cursor stability | new postings at the head do not shift an in-flight cursor |
| Page-size cap | `first: 1000000` returns at most 100 |
| Batch loading | a page of statement lines resolves `transaction` in one query |

## Observability

Timers and counters live **inside** the services that record them — a config class cannot time a
method body. `ObservabilityConfig` makes them usable: SLO histogram buckets on the posting and
lock-acquisition timers (explicit buckets, because client-side percentiles cannot be aggregated
across instances), the `@Timed`/`@Counted` aspects, and a **cardinality guard** that denies any
ledger meter tagged with an account or transaction id. That last one is a guard against the easiest
way to take down a monitoring system from application code: one time series per account, growing
forever.

The two most important numbers the system produces are correctness, not health:

| Gauge | Must be | Meaning |
|---|---|---|
| `apex.ledger.invariant.unbalanced.currencies` | `0` | A currency whose journal does not sum to zero |
| `apex.ledger.invariant.drifted.accounts` | `0` | A projection disagreeing with the journal |
| `apex.ledger.invariant.last.check.timestamp` | advancing | Freshness — a stale gauge reads as healthy |

Both aggregate the whole journal, so they are computed on a **5-minute schedule** and published into
an `AtomicLong` the gauge reads. Wiring them directly to a Prometheus scrape would run a full-table
aggregation every fifteen seconds forever, and the monitoring would become the outage. They start at
`-1`, not `0`, so "never checked" is distinguishable from "verified healthy".

## Resilience

A Resilience4j rate limiter guards `postTransaction` at **200 permits/second per instance**, sized
against the real bottleneck — the 32-connection JDBC pool — not picked round. Two deliberate
properties:

- **Rejection is immediate** (`timeoutDuration = 0`). Queuing would convert a throughput problem
  into a latency one, and a client blocked for seconds may time out locally and resubmit while the
  original is still in flight.
- **The limit is per instance, not per cluster.** N replicas admit N × 200/s. That is correct for
  defending each process's own pool, but it is not a global quota; a cluster-wide limit would need
  shared state and a Redis round trip in front of every mutation.

It is admission control, never a correctness control — every ledger guarantee holds with it disabled.
A shed request writes nothing and does not consume its idempotency key, so the same request can be
retried with the same key.

## The concurrency suite

`LedgerConcurrencyIntegrationTest` — **zero mocks**: PostgreSQL, Redis and Kafka are all
Testcontainers. That is not a preference. Every guarantee is enforced by infrastructure — a trigger,
a CHECK constraint, a unique index, a Redisson lease — so a mocked repository would return whatever
the author expected and prove nothing.

Load is generated with `newVirtualThreadPerTaskExecutor` and released through a `CountDownLatch`
starting gate, so 50 requests actually collide instead of arriving spread over however long they take
to submit.

| Scenario | Assertion |
|---|---|
| 50 threads, same idempotency key | exactly **1** posting; balance moved once, not 50× |
| 10 distinct keys × 5 duplicates | exactly **10** postings, one per key |
| 50 threads racing a balance covering one | exactly **1** funded; floored at `0.00`, never negative |
| 50 threads, balance covering ten | balance equals `100.00 − 10.00 × successes` exactly |
| 50 opposing transfers on one pair | no deadlock; value conserved between the pair |
| 50 postings → outbox → Kafka | one event per committed transaction, all published |
| Rate limiter drained | `RATE_LIMITED`, nothing written, key not consumed |
| 50 concurrent mutations via GraphQL | balance equals exactly the number of accepted requests |

Every scenario ends on the same three database-side questions: does every currency balance, has any
projection drifted, is any account below its floor. Counting successes alone would pass on a ledger
that had also corrupted a balance.

Assertions tolerate **shed load** (a lock timeout or a rate-limit rejection) as a valid outcome that
writes nothing, rather than asserting contention never happens — which would be a claim about
scheduling luck, not correctness. Verified across two consecutive runs: 10/10 both times, with 3 lock
timeouts in one earlier run and 0 in these, and the assertions holding either way.

## Running the tests

```bash
mvn test                  # unit suite; no Docker required
mvn verify                # adds the four integration tests; needs a container runtime
mvn verify -DskipITs      # unit suite only
```

`*IT` and `*IntegrationTest` both run under failsafe. The latter is excluded from surefire
explicitly — it matches surefire's default `*Test` pattern, so without that exclusion a
container-backed test would run during `mvn test` and fail on any machine without Docker.
