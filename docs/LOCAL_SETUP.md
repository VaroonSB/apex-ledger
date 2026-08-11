# Running ApexLedger on a Mac

Verified image architectures: `postgres:16.14-alpine`, `redis:7.4.10-alpine` and `apache/kafka:4.1.2`
all publish native **`arm64`** manifests, so Apple Silicon runs them without Rosetta emulation.

---

## 1. Prerequisites

```bash
# Java 21 — required. Virtual threads (JEP 444) are final in 21; the build targets release 21.
brew install --cask temurin@21
java -version            # expect: openjdk version "21.x"

# Maven 3.9+
brew install maven
mvn -v

# A container runtime. Any ONE of these:
brew install --cask docker        # Docker Desktop — simplest
brew install colima               # or: lightweight, no GUI
brew install --cask orbstack      # or: fastest on Apple Silicon
```

If `java -version` reports something other than 21, set it explicitly:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

### Give the container runtime enough memory

The stack asks for real resources: PostgreSQL is configured with `shared_buffers=512MB` and Kafka runs
with `-Xmx1g`. **Docker Desktop's 2 GB default is not enough** — Kafka will be OOM-killed partway
through startup, which surfaces as a healthcheck that never turns healthy.

- **Docker Desktop:** Settings → Resources → Memory → **6 GB** minimum, 4 CPUs.
- **colima:** `colima start --cpu 4 --memory 6 --disk 40`

---

## 2. Clone and check out

```bash
git clone https://github.com/VaroonSB/apex-ledger.git
cd apex-ledger
git checkout claude/apexledger-phase-1-scaffold-ebqnys   # Phases 4–5; main has 1–3
```

## 3. Start the infrastructure

```bash
docker compose up -d
docker compose ps
```

**Wait for `STATUS` to read `healthy`, not merely `running`** — for all three. Kafka formats its KRaft
storage directory on first boot and can take 30–45 seconds. Starting the app against a `running` but
not-yet-`healthy` Kafka is the most common false start.

```bash
# Watch until all three are healthy
watch -n 2 'docker compose ps'
```

## 4. Build and run

```bash
mvn clean verify -DskipITs     # compile + unit suite, no containers needed
mvn spring-boot:run
```

Startup is only healthy if you see these three lines — each is an assertion that would otherwise fail
silently:

```
Redisson lock client configured for redis://localhost:6379 (database 0, ssl false)
Lock lease PT15S safely exceeds transaction timeout PT10S
rate limiter 'postTransaction' active: 200 permits per PT1S per instance, PT0S timeout
```

Flyway must report `Successfully applied 1 migration`, and Hibernate must **not** report a
schema-validation error — `ddl-auto: validate` means a mapping mismatch aborts the boot on purpose.

## 5. Smoke test

```bash
curl -s localhost:8080/graphql -H 'Content-Type: application/json' \
  -d '{"query":"{ ping }"}'
# => {"data":{"ping":"pong"}}
```

There is **no seeded chart of accounts** — the schema ships with no rows, and no mutation creates an
account. Insert two by hand to exercise a posting:

```bash
docker compose exec -T postgres psql -U apex -d apex_ledger <<'SQL'
INSERT INTO accounts (id, account_number, name, account_type, currency, minimum_balance) VALUES
 ('11111111-1111-1111-1111-111111111111','CASH-USD','Cash USD','ASSET','USD',NULL),
 ('22222222-2222-2222-2222-222222222222','CUST-USD','Customer USD','LIABILITY','USD',0);
SQL
```

Then post a transaction. Remember the convention: **source is CREDITED, destination is DEBITED.**

```bash
curl -s localhost:8080/graphql -H 'Content-Type: application/json' -d '{
  "query": "mutation($i: PostTransactionInput!) { postTransaction(input: $i) { replayed transaction { id } balancesAfter { accountNumber balance } } }",
  "variables": { "i": {
    "sourceAccountId": "22222222-2222-2222-2222-222222222222",
    "destinationAccountId": "11111111-1111-1111-1111-111111111111",
    "amount": "100.00", "currency": "USD", "idempotencyKey": "demo-001" } }
}'
```

Run the identical command again — `replayed` flips to `true`, the transaction id is unchanged, and no
second posting occurs. That is idempotency working.

Then confirm the event reached Kafka (the relay polls every 500 ms):

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic apex.ledger.journal-entries.v1 --from-beginning --max-messages 1
```

The application log should also show an `AUDIT type=TransactionSettled ...` line from the consumer.

Useful endpoints: <http://localhost:8080/graphiql> · `/actuator/health` ·
`/actuator/metrics/apex.ledger.posting` · `/actuator/prometheus`

## 6. Run the full test suite

```bash
mvn verify          # adds the four integration tests; needs the container runtime
```

This is the **one step never executed in the environment where this code was written** — there was no
Docker daemon available. Expect to spend a few minutes on first run pulling images. If anything in
this project is going to fail for you, it is most likely here, and most likely container wiring rather
than ledger logic.

### If you use colima or OrbStack

Testcontainers finds the daemon through `DOCKER_HOST`:

```bash
# colima
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock

# OrbStack usually needs nothing; if it does:
export DOCKER_HOST="unix://${HOME}/.orbstack/run/docker.sock"
```

---

## Checklist once it is running locally

### Verify (in this order)

- [ ] `docker compose ps` — all three **healthy**, not just running
- [ ] `mvn clean verify -DskipITs` — green
- [ ] App boots; the three startup assertion lines above appear
- [ ] `{ ping }` returns `pong`
- [ ] A posting succeeds; the same request replays with `replayed: true`
- [ ] The event appears on the Kafka topic and in the audit log
- [ ] **`mvn verify`** — the four integration tests pass. The `apache/kafka:4.1.2` Testcontainer in
      `LedgerConcurrencyIntegrationTest` has never been started in any form; this is its first run.
- [ ] `curl -s localhost:8080/actuator/metrics/apex.ledger.invariant.unbalanced.currencies` — the
      value must be `0`, and **not `-1`**. `-1` means the 5-minute reconciliation has not completed
      yet; wait for it. A gauge stuck at `-1` means the scheduler is not running.

### Confirm two design decisions are what you actually want

- [ ] **Mutation direction.** `source` is credited, `destination` is debited. Whether that raises or
      lowers a balance depends on the account type — a debit increases an ASSET and decreases a
      LIABILITY. If your house convention is the opposite, this is a one-line change in
      `LedgerGraphqlController` plus the schema documentation.
- [ ] **The Redis lock is not the correctness mechanism.** PostgreSQL's balance trigger and
      `ck_accounts_minimum_balance` are. If you intended Redis to be the guard, that needs discussing
      before anything is built on top.

### Before this goes anywhere real

Ordered by how much damage the omission causes.

- [ ] **No authentication.** Every transaction records `createdBy = "graphql-api:unauthenticated"`.
      The API is entirely open — anyone who can reach the port can move money.
- [ ] **Kafka replication factor is 1.** One broker loss is permanent loss of the audit stream. Set
      RF ≥ 3 with `min.insync.replicas=2` in `KafkaConfig`.
- [ ] **Credentials are in `docker-compose.yml`.** `apex_local_dev` is a development value. Every
      endpoint is overridable by environment variable (`APEX_DB_PASSWORD`, `APEX_REDIS_PASSWORD`,
      `APEX_KAFKA_BOOTSTRAP_SERVERS`, …) — wire them to a secret store.
- [ ] **Actuator is unauthenticated**, including `/actuator/env` and `/actuator/threaddump`.
- [ ] **GraphiQL is enabled** (`spring.graphql.graphiql.enabled: true`). Disable it in deployed
      environments.
- [ ] **No account-management API.** Accounts can only be created by direct SQL.
- [ ] **`apex.ledger.topics.balance-projections` is declared but nothing consumes it.** The compacted
      topic exists; the projection consumer does not.
- [ ] **The rate limiter is per instance.** N replicas admit N × 200/sec. Not a cluster quota.
- [ ] **Alert on these**, in priority order:
      `apex.ledger.invariant.unbalanced.currencies` ≠ 0 · `apex.ledger.invariant.drifted.accounts`
      ≠ 0 · `apex.ledger.invariant.last.check.timestamp` not advancing ·
      `apex.ledger.lock.lease.expired` > 0 · `apex.ledger.outbox.backlog` rising ·
      `apex.ledger.outbox.relay{outcome="abandoned"}` > 0 · `apex.ledger.api.rate.limited` sustained
- [ ] **Write a runbook for an abandoned outbox event.** After 10 failed attempts the relay gives up
      and logs `ABANDONED`. That is a committed ledger change consumers will never see, and it needs a
      human — there is no automatic recovery.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| Kafka never turns `healthy` | Container runtime memory too low. Raise to 6 GB. |
| `port is already allocated` on 5432 | A local PostgreSQL (often Postgres.app) is running. Stop it, or remap the port in `docker-compose.yml`. |
| Same on 6379 / 9092 / 8080 | `brew services stop redis`, or `lsof -i :8080` to find the holder. |
| `Could not find a valid Docker environment` | The runtime is not running, or `DOCKER_HOST` is unset for colima/OrbStack. |
| `Unsupported Database: PostgreSQL` | Only if `flyway-database-postgresql` was removed — Flyway 10+ needs the engine module. |
| Hibernate `Schema-validation` failure at boot | An entity no longer matches the migration. Intentional: `ddl-auto: validate` refuses to start on drift. |
| `docker compose down -v` | **Destroys all ledger data.** The journal is append-only but the volume is not. |
| Tests cannot clean up between runs | Correct. The append-only triggers reject `DELETE` and `TRUNCATE` on `journal_entries` and `transactions`; isolation comes from a fresh container per run. |
