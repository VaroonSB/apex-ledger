-- ===========================================================================
-- ApexLedger — V1: initial ledger schema
--
-- Four tables:
--   accounts        chart of accounts + DB-maintained balance projection
--   transactions    the immutable business-level header, carries the
--                   idempotency key
--   journal_entries the immutable double-entry postings (append-only)
--   outbox_events   transactional outbox for at-least-once event publication
--
-- Design principles encoded below, in order of importance:
--
--  1. The journal is APPEND-ONLY. Enforced three ways: no UPDATE/DELETE code
--     path, `updatable = false` on every JPA column, and a database trigger
--     that raises on any UPDATE, DELETE or TRUNCATE. A correction is a new
--     compensating transaction, never an edit.
--
--  2. Double-entry balance is a DATABASE invariant, not an application
--     convention. A deferred constraint trigger verifies at COMMIT that every
--     transaction sums to zero per currency. No application bug can commit a
--     lopsided entry.
--
--  3. Account balances are maintained BY THE DATABASE from the journal. A
--     trigger folds each inserted entry into the account's running totals, so
--     the projection cannot drift from the journal, and the minimum-balance
--     CHECK then rejects an overdraft as a constraint violation. That same
--     UPDATE takes a row lock, which serialises concurrent postings against
--     one account — the storage-layer half of double-spend prevention.
--
--  4. Money is NUMERIC. Never a float. Amounts are always POSITIVE; the
--     debit/credit direction carries the sign.
--
-- Currency scale note: PostgreSQL NUMERIC(38,18) silently ROUNDS an input with
-- more than 18 fraction digits rather than rejecting it, and the database has
-- no currency registry telling it that USD permits 2 digits and JPY permits 0.
-- Per-currency scale is therefore enforced in the domain by Money/CurrencyCode,
-- which reject any amount finer than the currency's minor unit. 18 digits is
-- far beyond any ISO-4217 currency, so the rounding path is unreachable for
-- valid input.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- Shared guard: append-only enforcement.
--
-- Statement-level so it fires even for a zero-row UPDATE, and covers TRUNCATE,
-- which row-level triggers cannot see. ERRCODE 0A000 (feature_not_supported)
-- is mapped to a typed exception in the persistence layer.
-- ---------------------------------------------------------------------------
CREATE FUNCTION apex_forbid_mutation() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION
        'ApexLedger: % on table "%" is forbidden; this table is append-only',
        TG_OP, TG_TABLE_NAME
        USING ERRCODE = '0A000',
              HINT = 'Record a compensating entry instead of modifying history.';
END;
$$;

COMMENT ON FUNCTION apex_forbid_mutation() IS
    'Raises unconditionally. Attached to append-only ledger tables to make history physically immutable.';


-- ---------------------------------------------------------------------------
-- Shared helper: maintain updated_at without trusting the application clock.
-- ---------------------------------------------------------------------------
CREATE FUNCTION apex_touch_updated_at() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;


-- ===========================================================================
-- accounts — chart of accounts
--
-- The only table with mutable rows. Two distinct kinds of mutation:
--   * status / name  — application-driven, guarded by the `version` column
--   * money totals   — DATABASE-driven only, via trg_journal_entries_apply.
--                      The application never writes these columns; they are
--                      mapped read-only in JPA.
-- ===========================================================================
CREATE TABLE accounts
(
    id              UUID           NOT NULL DEFAULT gen_random_uuid(),
    account_number  VARCHAR(64)    NOT NULL,
    name            VARCHAR(255)   NOT NULL,
    account_type    VARCHAR(16)    NOT NULL,
    currency        VARCHAR(3)     NOT NULL,
    status          VARCHAR(16)    NOT NULL DEFAULT 'ACTIVE',

    -- Monotonically increasing lifetime totals, folded in by trigger. Storing
    -- both sides rather than one net balance keeps the projection additive,
    -- which makes reconciliation against the journal a straight comparison and
    -- means a posting never has to read the previous balance to compute a
    -- delta.
    total_debits    NUMERIC(38, 18) NOT NULL DEFAULT 0,
    total_credits   NUMERIC(38, 18) NOT NULL DEFAULT 0,

    -- Signed balance, sign convention taken from the account's natural side.
    -- Generated rather than computed in Java so there is exactly ONE definition
    -- of "balance"; Account maps this column read-only instead of re-deriving it.
    -- The expression does appear twice, but only within this file: the CHECK
    -- below has to repeat it, because PostgreSQL forbids referencing a generated
    -- column from a CHECK constraint on the same table.
    balance         NUMERIC(38, 18) NOT NULL GENERATED ALWAYS AS (
                        CASE
                            WHEN account_type IN ('ASSET', 'EXPENSE')
                                THEN total_debits - total_credits
                            ELSE total_credits - total_debits
                        END
                    ) STORED,

    -- NULL means "no floor" (e.g. a system clearing account that is expected
    -- to run negative). A non-NULL value is enforced by ck_accounts_minimum_balance.
    minimum_balance NUMERIC(38, 18),

    version         BIGINT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT pk_accounts PRIMARY KEY (id),
    CONSTRAINT uq_accounts_account_number UNIQUE (account_number),

    -- Redundant against the primary key, but required as the target of the
    -- composite foreign key from journal_entries. That FK is what guarantees,
    -- declaratively, that an entry's currency always equals its account's
    -- currency — no trigger and no application check needed.
    CONSTRAINT uq_accounts_id_currency UNIQUE (id, currency),

    CONSTRAINT ck_accounts_type
        CHECK (account_type IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE')),
    CONSTRAINT ck_accounts_status
        CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    CONSTRAINT ck_accounts_currency_format
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_accounts_account_number_not_blank
        CHECK (length(btrim(account_number)) > 0),
    CONSTRAINT ck_accounts_name_not_blank
        CHECK (length(btrim(name)) > 0),

    -- Totals are lifetime sums of positive amounts and can only grow.
    CONSTRAINT ck_accounts_totals_non_negative
        CHECK (total_debits >= 0 AND total_credits >= 0),
    CONSTRAINT ck_accounts_version_non_negative
        CHECK (version >= 0),

    -- THE OVERDRAFT GUARD. Repeats the balance expression instead of
    -- referencing the generated `balance` column, because PostgreSQL does not
    -- allow a generated column inside a CHECK constraint on the same table.
    -- Because the balance trigger UPDATEs this row inside the posting
    -- transaction, this constraint fires there: an overdraft aborts the
    -- transfer at the storage layer even if every application check is wrong.
    CONSTRAINT ck_accounts_minimum_balance
        CHECK (
            minimum_balance IS NULL
                OR (CASE
                        WHEN account_type IN ('ASSET', 'EXPENSE')
                            THEN total_debits - total_credits
                        ELSE total_credits - total_debits
                    END) >= minimum_balance
        )
);

COMMENT ON TABLE accounts IS
    'Chart of accounts. total_debits/total_credits/balance are a database-maintained projection of journal_entries and are never written by the application.';
COMMENT ON COLUMN accounts.balance IS
    'Signed balance in the account currency, positive on the account type''s natural side.';
COMMENT ON COLUMN accounts.minimum_balance IS
    'Hard floor enforced by ck_accounts_minimum_balance. NULL disables the floor.';
COMMENT ON COLUMN accounts.version IS
    'JPA optimistic lock. Guards status/name edits only; money movement is guarded pessimistically.';

CREATE INDEX idx_accounts_currency_type ON accounts (currency, account_type);
CREATE INDEX idx_accounts_status ON accounts (status) WHERE status <> 'ACTIVE';

CREATE TRIGGER trg_accounts_touch_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION apex_touch_updated_at();


-- ===========================================================================
-- transactions — immutable business-level header
--
-- One row per accepted business operation. Insert-only: a reversal is a NEW
-- row pointing back at the original through reverses_transaction_id, never a
-- status flip on the original.
--
-- This table carries the idempotency key, which means the INSERT *is* the
-- idempotency reservation — atomic with the postings by construction, with no
-- separate reservation table to leak or to fall out of sync.
-- ===========================================================================
CREATE TABLE transactions
(
    id                      UUID         NOT NULL DEFAULT gen_random_uuid(),

    -- Client-supplied. UNIQUE below is the authoritative duplicate-submission
    -- guard; the Redis SET NX fast path only exists to avoid reaching the
    -- database for an obvious replay.
    idempotency_key         VARCHAR(255) NOT NULL,

    -- SHA-256 of the canonical request body. Distinguishes an honest retry
    -- (same key, same fingerprint) from a key collision or client bug (same
    -- key, different fingerprint). Without it, replaying a key with a
    -- different payload would silently return the wrong original result.
    request_fingerprint     VARCHAR(64)  NOT NULL,

    kind                    VARCHAR(16)  NOT NULL,
    reference               VARCHAR(128),
    description             VARCHAR(512),

    -- Set only on kind = 'REVERSAL'. UNIQUE permits many NULLs in PostgreSQL
    -- while allowing at most one reversal per original transaction.
    reverses_transaction_id UUID,

    -- Bitemporal: effective_at is the business date the entry belongs to,
    -- created_at is when the system recorded it. Backdated corrections need
    -- both to remain auditable.
    effective_at            TIMESTAMPTZ  NOT NULL,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_by              VARCHAR(128) NOT NULL,

    CONSTRAINT pk_transactions PRIMARY KEY (id),
    CONSTRAINT uq_transactions_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT uq_transactions_reverses UNIQUE (reverses_transaction_id),
    CONSTRAINT fk_transactions_reverses
        FOREIGN KEY (reverses_transaction_id) REFERENCES transactions (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,

    CONSTRAINT ck_transactions_kind
        CHECK (kind IN ('TRANSFER', 'ADJUSTMENT', 'REVERSAL')),
    CONSTRAINT ck_transactions_fingerprint_format
        CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_transactions_idempotency_key_not_blank
        CHECK (length(btrim(idempotency_key)) > 0),
    CONSTRAINT ck_transactions_created_by_not_blank
        CHECK (length(btrim(created_by)) > 0),

    -- A reversal must name its original, and nothing else may.
    CONSTRAINT ck_transactions_reversal_link
        CHECK ((kind = 'REVERSAL') = (reverses_transaction_id IS NOT NULL)),
    CONSTRAINT ck_transactions_no_self_reversal
        CHECK (reverses_transaction_id IS DISTINCT FROM id)
);

COMMENT ON TABLE transactions IS
    'Immutable business-level header. Append-only, enforced by trg_transactions_append_only.';
COMMENT ON COLUMN transactions.idempotency_key IS
    'Client-supplied duplicate-submission guard. The unique constraint is the authority.';
COMMENT ON COLUMN transactions.request_fingerprint IS
    'Lowercase hex SHA-256 of the canonical request, used to tell an honest retry from key reuse.';

CREATE INDEX idx_transactions_created_at ON transactions (created_at DESC);
CREATE INDEX idx_transactions_effective_at ON transactions (effective_at DESC);
CREATE INDEX idx_transactions_reference ON transactions (reference) WHERE reference IS NOT NULL;

CREATE TRIGGER trg_transactions_append_only
    BEFORE UPDATE OR DELETE ON transactions
    FOR EACH STATEMENT
    EXECUTE FUNCTION apex_forbid_mutation();

CREATE TRIGGER trg_transactions_no_truncate
    BEFORE TRUNCATE ON transactions
    FOR EACH STATEMENT
    EXECUTE FUNCTION apex_forbid_mutation();


-- ===========================================================================
-- journal_entries — the immutable double-entry postings
--
-- The audit record of the entire system. Every other money column anywhere is
-- a projection of this table and must be reconcilable against it.
-- ===========================================================================
CREATE TABLE journal_entries
(
    id             UUID           NOT NULL DEFAULT gen_random_uuid(),
    transaction_id UUID           NOT NULL,
    account_id     UUID           NOT NULL,

    -- Position within the transaction. Makes the entry set deterministically
    -- ordered for replay and gives the (transaction_id, entry_sequence) unique
    -- key that stops the same posting being written twice.
    entry_sequence SMALLINT       NOT NULL,

    direction      VARCHAR(6)     NOT NULL,

    -- Always strictly positive. Sign lives in `direction`; a negative amount
    -- would make every SUM in the system ambiguous.
    amount         NUMERIC(38, 18) NOT NULL,

    -- Denormalised from the account so the composite FK below can prove the
    -- two always agree.
    currency       VARCHAR(3)     NOT NULL,

    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT pk_journal_entries PRIMARY KEY (id),

    CONSTRAINT fk_journal_entries_transaction
        FOREIGN KEY (transaction_id) REFERENCES transactions (id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,

    -- Composite FK: guarantees currency always matches the account's currency.
    CONSTRAINT fk_journal_entries_account_currency
        FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency)
        ON DELETE RESTRICT ON UPDATE RESTRICT,

    CONSTRAINT uq_journal_entries_transaction_sequence
        UNIQUE (transaction_id, entry_sequence),

    CONSTRAINT ck_journal_entries_direction
        CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ck_journal_entries_amount_positive
        CHECK (amount > 0),
    CONSTRAINT ck_journal_entries_sequence_non_negative
        CHECK (entry_sequence >= 0),
    CONSTRAINT ck_journal_entries_currency_format
        CHECK (currency ~ '^[A-Z]{3}$')
);

COMMENT ON TABLE journal_entries IS
    'Append-only double-entry postings. The audit truth of the ledger; every balance elsewhere is a projection of this table.';
COMMENT ON COLUMN journal_entries.amount IS
    'Strictly positive. Debit/credit sign is carried by the direction column.';

-- Statement history and balance reconciliation for one account, newest first.
CREATE INDEX idx_journal_entries_account_created
    ON journal_entries (account_id, created_at DESC, id);

-- (transaction_id, entry_sequence) is already covered by the unique constraint,
-- so no separate index on transaction_id is created.

CREATE TRIGGER trg_journal_entries_append_only
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH STATEMENT
    EXECUTE FUNCTION apex_forbid_mutation();

CREATE TRIGGER trg_journal_entries_no_truncate
    BEFORE TRUNCATE ON journal_entries
    FOR EACH STATEMENT
    EXECUTE FUNCTION apex_forbid_mutation();


-- ---------------------------------------------------------------------------
-- Balance projection: fold each posting into its account.
--
-- Runs AFTER INSERT on every entry. Three things fall out of this:
--   * accounts.total_* can never drift from the journal, because the same
--     statement that writes an entry updates the projection;
--   * the UPDATE takes a row lock on accounts, serialising concurrent
--     postings against the same account;
--   * ck_accounts_minimum_balance is evaluated here, so an overdraft aborts
--     the transaction at the storage layer.
--
-- Also rejects postings to a non-ACTIVE account, which is cheaper and more
-- reliable here than a separate application read.
-- ---------------------------------------------------------------------------
CREATE FUNCTION apex_apply_entry_to_account_balance() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
DECLARE
    v_status  accounts.status%TYPE;
    v_updated INTEGER;
BEGIN
    UPDATE accounts
       SET total_debits  = total_debits
                           + CASE WHEN NEW.direction = 'DEBIT' THEN NEW.amount ELSE 0 END,
           total_credits = total_credits
                           + CASE WHEN NEW.direction = 'CREDIT' THEN NEW.amount ELSE 0 END
     WHERE id = NEW.account_id
       AND status = 'ACTIVE'
    RETURNING status INTO v_status;

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    IF v_updated = 0 THEN
        -- Either the account is gone (impossible: FK) or it is not ACTIVE.
        SELECT status INTO v_status FROM accounts WHERE id = NEW.account_id;
        -- The constraint name is embedded in the message text using PostgreSQL's own
        -- phrasing, not just passed via USING CONSTRAINT. A plpgsql RAISE populates the
        -- error's constraint FIELD, but that field is only reachable through the
        -- driver-specific PSQLException API — and it is absent entirely from the exception
        -- Spring produces for a COMMIT-time failure. Putting the name in the message lets
        -- one parser in ConstraintViolations handle statement-time and commit-time errors
        -- alike, without a compile dependency on the JDBC driver.
        RAISE EXCEPTION
            'ApexLedger: account % violates check constraint "ck_accounts_status_postable": status is % (must be ACTIVE); cannot post entry %',
            NEW.account_id, COALESCE(v_status, 'MISSING'), NEW.id
            USING ERRCODE = '23514',
                  CONSTRAINT = 'ck_accounts_status_postable';
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER trg_journal_entries_apply_balance
    AFTER INSERT ON journal_entries
    FOR EACH ROW
    EXECUTE FUNCTION apex_apply_entry_to_account_balance();


-- ---------------------------------------------------------------------------
-- THE double-entry invariant: every transaction sums to zero, per currency.
--
-- A DEFERRABLE INITIALLY DEFERRED constraint trigger, so it runs at COMMIT
-- rather than after each INSERT. That is what allows the entries of one
-- transaction to be inserted in any order, or in a JDBC batch, and still be
-- checked as a set.
--
-- Per-currency rather than in aggregate: a genuine FX transfer books through
-- an FX position account so that each currency balances independently. A rule
-- that only required the grand total to be zero would accept debiting 100 USD
-- and crediting 100 JPY.
-- ---------------------------------------------------------------------------
CREATE FUNCTION apex_assert_transaction_balanced() RETURNS TRIGGER
    LANGUAGE plpgsql AS $$
DECLARE
    v_currency    journal_entries.currency%TYPE;
    v_imbalance   NUMERIC(38, 18);
    v_entry_count INTEGER;
BEGIN
    SELECT count(*) INTO v_entry_count
      FROM journal_entries
     WHERE transaction_id = NEW.transaction_id;

    -- Single-sided postings are never valid double-entry.
    IF v_entry_count < 2 THEN
        RAISE EXCEPTION
            'ApexLedger: transaction % violates check constraint "ck_journal_entries_balanced": has % journal entr%, double-entry requires at least 2',
            NEW.transaction_id, v_entry_count,
            CASE WHEN v_entry_count = 1 THEN 'y' ELSE 'ies' END
            USING ERRCODE = '23514',
                  CONSTRAINT = 'ck_journal_entries_balanced';
    END IF;

    FOR v_currency, v_imbalance IN
        SELECT currency,
               sum(CASE WHEN direction = 'DEBIT' THEN amount ELSE -amount END)
          FROM journal_entries
         WHERE transaction_id = NEW.transaction_id
         GROUP BY currency
    LOOP
        IF v_imbalance <> 0 THEN
            RAISE EXCEPTION
                'ApexLedger: transaction % violates check constraint "ck_journal_entries_balanced": unbalanced in %, debits - credits = %',
                NEW.transaction_id, v_currency, v_imbalance
                USING ERRCODE = '23514',
                      CONSTRAINT = 'ck_journal_entries_balanced',
                      HINT = 'Every transaction must sum to zero independently in each currency.';
        END IF;
    END LOOP;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_journal_entries_balanced
    AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION apex_assert_transaction_balanced();


-- ===========================================================================
-- outbox_events — transactional outbox
--
-- Written in the SAME transaction as the journal entries, so an event exists
-- if and only if the ledger change committed. A relay then moves PENDING rows
-- to Kafka at-least-once. This table is deliberately MUTABLE: status, attempts
-- and published_at are dispatch bookkeeping, not ledger history.
--
-- BIGINT IDENTITY rather than a UUID primary key because the relay reads in
-- primary-key order and wants dense, insertion-ordered keys. Accepted cost:
-- Hibernate cannot batch inserts for an IDENTITY entity. That is fine at one
-- outbox row per ledger transaction.
-- ===========================================================================
CREATE TABLE outbox_events
(
    id             BIGINT       NOT NULL GENERATED BY DEFAULT AS IDENTITY,

    -- Stable business identity of the event, carried to Kafka so consumers can
    -- deduplicate across the at-least-once redeliveries this pattern implies.
    event_id       UUID         NOT NULL DEFAULT gen_random_uuid(),

    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   UUID         NOT NULL,
    event_type     VARCHAR(128) NOT NULL,

    topic          VARCHAR(255) NOT NULL,

    -- Kafka message key. Keying by account keeps all events for one account on
    -- one partition, which is what preserves per-account ordering downstream.
    partition_key  VARCHAR(255) NOT NULL,

    payload        JSONB        NOT NULL,
    headers        JSONB        NOT NULL DEFAULT '{}'::jsonb,

    status         VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    attempts       INTEGER      NOT NULL DEFAULT 0,

    -- Retry backoff gate: the relay only claims rows whose available_at has
    -- passed, so a failing event backs off instead of spinning.
    available_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),

    occurred_at    TIMESTAMPTZ  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    last_error     TEXT,
    version        BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT pk_outbox_events PRIMARY KEY (id),
    CONSTRAINT uq_outbox_events_event_id UNIQUE (event_id),

    CONSTRAINT ck_outbox_events_status
        CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED', 'ABANDONED')),
    CONSTRAINT ck_outbox_events_attempts_non_negative
        CHECK (attempts >= 0),
    CONSTRAINT ck_outbox_events_version_non_negative
        CHECK (version >= 0),
    CONSTRAINT ck_outbox_events_topic_not_blank
        CHECK (length(btrim(topic)) > 0),
    CONSTRAINT ck_outbox_events_partition_key_not_blank
        CHECK (length(btrim(partition_key)) > 0),

    -- published_at is set exactly when, and only when, status is PUBLISHED.
    CONSTRAINT ck_outbox_events_published_at
        CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL)),

    -- payload must be a JSON object, not a bare scalar or array.
    CONSTRAINT ck_outbox_events_payload_is_object
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_outbox_events_headers_is_object
        CHECK (jsonb_typeof(headers) = 'object')
);

COMMENT ON TABLE outbox_events IS
    'Transactional outbox. Rows are written in the ledger transaction and relayed to Kafka at-least-once. Mutable dispatch state only.';
COMMENT ON COLUMN outbox_events.available_at IS
    'Retry gate. The relay claims only rows with available_at <= now().';
COMMENT ON COLUMN outbox_events.partition_key IS
    'Kafka record key; determines partition and therefore ordering guarantees.';

-- The relay's only hot query: oldest claimable pending rows. Partial index so
-- it stays small no matter how much PUBLISHED history accumulates.
CREATE INDEX idx_outbox_events_claimable
    ON outbox_events (available_at, id)
    WHERE status IN ('PENDING', 'FAILED');

-- Trace every event emitted by one aggregate.
CREATE INDEX idx_outbox_events_aggregate
    ON outbox_events (aggregate_type, aggregate_id, id);

-- Supports pruning published history.
CREATE INDEX idx_outbox_events_published_at
    ON outbox_events (published_at)
    WHERE status = 'PUBLISHED';
