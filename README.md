# ApexLedger

A production-grade, highly concurrent, multi-currency **double-entry ledger engine**.

Spring Boot 3.5.16 · Java 21 (virtual threads) · Spring GraphQL · PostgreSQL 16 · Redis 7 · Apache
Kafka 4 (KRaft)

> **Status: Phase 3 complete — concurrency engine, distributed locking and balance cache.** The
> posting engine, Redisson account locking and the read-through/write-through balance cache are in
> place and verified against a real PostgreSQL 16 and Redis 7. The GraphQL API and the Kafka outbox
> relay arrive in Phase 4.

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

## Phase 4 (not yet implemented)

GraphQL domain schema, custom scalars and typed error mapping · the Kafka outbox relay draining
`outbox_events` with `SKIP LOCKED` · consumer-side balance projections · Redis `SET NX` idempotency
fast path.
