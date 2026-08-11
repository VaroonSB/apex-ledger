package com.apex.ledger.infrastructure.persistence.repository;

import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.infrastructure.persistence.entity.Transaction;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Persistence access for the append-only {@link Transaction} header.
 *
 * <p>Read and insert only. There is no {@code delete}, and no update path: recorded history is never
 * revised, and {@code trg_transactions_append_only} would reject the attempt anyway. Narrowing the
 * interface moves that from a runtime failure to something that does not compile.
 */
public interface TransactionRepository extends Repository<Transaction, UUID> {

    Transaction save(Transaction transaction);

    Optional<Transaction> findById(UUID id);

    /**
     * The idempotency lookup.
     *
     * <p>Used as the fast path before attempting an insert. Backed by
     * {@code uq_transactions_idempotency_key}, so this is an index probe.
     *
     * <p>Takes the {@link IdempotencyKey} value object, not a {@code String}. That is not merely
     * stylistic: the entity field is mapped through {@code IdempotencyKeyConverter}, so Spring Data
     * binds this parameter as the converted type. Declaring it as {@code String} compiles fine and
     * then fails at runtime with {@code Argument [...] of type [java.lang.String] did not match
     * parameter type [IdempotencyKey]} — a defect no compiler catches.
     */
    Optional<Transaction> findByIdempotencyKey(IdempotencyKey idempotencyKey);

    boolean existsByIdempotencyKey(IdempotencyKey idempotencyKey);

    /** Resolves whether a transaction has already been reversed, per {@code uq_transactions_reverses}. */
    Optional<Transaction> findByReversesTransactionId(UUID reversesTransactionId);

    Page<Transaction> findAllByOrderByCreatedAtDesc(Pageable pageable);
}
