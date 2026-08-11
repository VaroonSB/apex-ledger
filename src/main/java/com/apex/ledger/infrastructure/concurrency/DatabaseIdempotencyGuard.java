package com.apex.ledger.infrastructure.concurrency;

import com.apex.ledger.application.port.out.IdempotencyGuard;
import com.apex.ledger.domain.exception.IdempotencyConflictException;
import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.domain.model.RequestFingerprint;
import com.apex.ledger.infrastructure.persistence.ConstraintViolations;
import com.apex.ledger.infrastructure.persistence.DatabaseConstraints;
import com.apex.ledger.infrastructure.persistence.entity.Transaction;
import com.apex.ledger.infrastructure.persistence.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Idempotency enforced by the {@code uq_transactions_idempotency_key} unique constraint.
 *
 * <h2>Why the constraint, and not a lookup</h2>
 *
 * <p>The obvious implementation — look the key up, and insert if absent — is wrong under concurrency.
 * Two requests carrying the same key can both find nothing and both proceed, which is precisely the
 * double-post the guard exists to prevent. No amount of care in the application closes that window,
 * because the two statements are separate.
 *
 * <p>So the unique constraint is the mechanism, and the lookup is only an optimisation. The constraint
 * is checked by the database at the moment of insert, atomically, and it holds no matter how many
 * application instances race. This is also why the key lives on {@code transactions} rather than in a
 * dedicated reservation table: claiming the key and writing the postings become the same atomic act,
 * so there is no reservation that can outlive a failed posting and block the client's legitimate retry.
 *
 * <h2>Why a conflict rolls the transaction back</h2>
 *
 * <p>When the unique violation fires, the enclosing JDBC transaction is unusable — PostgreSQL will
 * reject every subsequent statement in it until rollback. This class therefore does not attempt to
 * recover and continue; it identifies the conflict and rethrows so the transaction unwinds. That is
 * the desired semantics: the duplicate submission must leave nothing behind.
 *
 * <p>A consequence worth stating plainly: identifying the winning transaction after a violation
 * requires a query, and that query cannot run in the poisoned transaction. {@link #execute} therefore
 * reports {@link IdempotencyConflictException.Reason#CONCURRENT_SUBMISSION} — retryable — rather than
 * guessing at the winner. A retry runs {@link #assertUnused} in a clean transaction and gets the
 * precise answer. Failing honestly and retryably beats reporting a specific outcome derived from a
 * broken connection.
 *
 * <h2>Failed attempts do not consume keys</h2>
 *
 * <p>Because the reservation is the transaction row itself, a rollback releases the key. A client
 * whose transfer failed validation may resubmit with the same key. This is intentional: the guard
 * exists to prevent a request being applied twice, not to prevent it being attempted twice.
 */
@Component
public class DatabaseIdempotencyGuard implements IdempotencyGuard {

    private static final Logger log = LoggerFactory.getLogger(DatabaseIdempotencyGuard.class);

    private final TransactionRepository transactions;

    public DatabaseIdempotencyGuard(TransactionRepository transactions) {
        this.transactions = Objects.requireNonNull(transactions, "transactions must not be null");
    }

    @Override
    public void assertUnused(IdempotencyKey key, RequestFingerprint fingerprint) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(fingerprint, "fingerprint must not be null");

        transactions.findByIdempotencyKey(key)
                .ifPresent(existing -> {
                    throw conflictFor(key, fingerprint, existing);
                });
    }

    @Override
    public <T> T execute(IdempotencyKey key, RequestFingerprint fingerprint, Supplier<T> action) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(fingerprint, "fingerprint must not be null");
        Objects.requireNonNull(action, "action must not be null");

        // Fast path. Catches the overwhelmingly common case — a client retrying after a timeout —
        // without provoking a constraint violation and poisoning the transaction.
        assertUnused(key, fingerprint);

        try {
            return action.get();
        } catch (DataIntegrityViolationException e) {
            if (isIdempotencyKeyViolation(e)) {
                // Lost the race. The transaction is already doomed, so the winner cannot be read
                // here; report a retryable conflict and let a clean transaction resolve it.
                log.debug("idempotency key '{}' was claimed concurrently", key, e);
                throw IdempotencyConflictException.concurrentSubmission(key, e);
            }
            // A different integrity rule was broken — an overdraft, an unbalanced entry, a currency
            // mismatch. Not this class's concern; propagate for the persistence-level translator.
            throw e;
        }
    }

    /**
     * Classifies an existing row as a benign replay or as key reuse.
     *
     * <p>The fingerprint comparison is the whole point. Without it a client that reused a key for a
     * different transfer would be handed the first transfer's result and told it succeeded — reporting
     * a movement of money that never happened.
     */
    private IdempotencyConflictException conflictFor(
            IdempotencyKey key, RequestFingerprint fingerprint, Transaction existing) {
        if (existing.matchesFingerprint(fingerprint)) {
            log.debug("idempotency key '{}' replays transaction {}", key, existing.getId());
            return IdempotencyConflictException.duplicate(key, existing.getId());
        }
        log.warn("idempotency key '{}' reused with a different request; "
                        + "existing transaction {} has fingerprint {}, request presented {}",
                key, existing.getId(), existing.getRequestFingerprint(), fingerprint);
        return IdempotencyConflictException.payloadMismatch(key, existing.getId());
    }

    /**
     * Whether this violation is specifically the idempotency-key constraint.
     *
     * <p>Matches on constraint name when the driver reports one. If it does not, the fallback is
     * deliberately conservative: an unnamed unique violation is <em>not</em> attributed to this
     * constraint, because misattributing, say, a duplicate account number as an idempotency conflict
     * would tell a client their request was already processed when it never was. An unrecognised
     * violation propagates unchanged instead.
     */
    private boolean isIdempotencyKeyViolation(DataIntegrityViolationException e) {
        Optional<String> constraint = ConstraintViolations.constraintName(e);
        if (constraint.isPresent()) {
            return constraint.get()
                    .equals(DatabaseConstraints.UQ_TRANSACTIONS_IDEMPOTENCY_KEY);
        }
        if (ConstraintViolations.isUniqueViolation(e)) {
            log.warn("unique violation carried no constraint name; not attributing it to {}. "
                            + "Check the dialect's constraint-name extraction.",
                    DatabaseConstraints.UQ_TRANSACTIONS_IDEMPOTENCY_KEY, e);
        }
        return false;
    }
}
