package com.apex.ledger.infrastructure.persistence.repository;

import com.apex.ledger.infrastructure.persistence.entity.Account;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence access for {@link Account}.
 *
 * <p>Extends {@link Repository} rather than {@code JpaRepository} so the exposed surface is exactly
 * what the ledger needs. Notably absent: any delete operation. An account with history cannot be
 * deleted — {@code journal_entries} holds a {@code RESTRICT} foreign key to it — so offering
 * {@code delete()} would only advertise an operation that always fails.
 */
public interface AccountRepository extends Repository<Account, UUID> {

    Account save(Account account);

    Optional<Account> findById(UUID id);

    Optional<Account> findByAccountNumber(String accountNumber);

    boolean existsByAccountNumber(String accountNumber);

    Page<Account> findAll(Pageable pageable);

    /**
     * Loads an account holding a {@code PESSIMISTIC_WRITE} row lock ({@code SELECT ... FOR UPDATE}).
     *
     * <p>This is the entry point for any operation whose correctness depends on the current balance.
     * The balance trigger already serialises concurrent postings, but taking the lock explicitly and
     * <em>first</em> is what makes lock acquisition order controllable.
     *
     * <p><strong>Deadlock avoidance is the caller's responsibility.</strong> A transfer touching more
     * than one account must lock them in a deterministic order — sort by {@link UUID} and call this
     * method once per account. Two concurrent transfers between the same pair of accounts in opposite
     * directions will otherwise deadlock.
     *
     * <p>No batch variant is offered on purpose. {@code WHERE id IN (...) ORDER BY id FOR UPDATE}
     * looks like it locks in sorted order, but PostgreSQL does not guarantee that locks are acquired
     * in the ordering the query requests, so such a method would give false confidence.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") UUID id);

    /**
     * Reads the current balance straight from the generated column, bypassing the persistence
     * context.
     *
     * <p>Necessary because a loaded {@link Account} carries a stale balance once entries have been
     * posted: the trigger updates the row without Hibernate's knowledge, so a cached instance is not
     * refreshed. Use inside the same transaction as {@link #findByIdForUpdate} to observe the value
     * the lock protects.
     */
    @Query(value = "select balance from accounts where id = :id", nativeQuery = true)
    Optional<BigDecimal> findCurrentBalance(@Param("id") UUID id);

    /**
     * Reconciliation: accounts whose stored projection disagrees with the journal.
     *
     * <p>Must always return zero rows. The balance trigger makes drift impossible by construction, so
     * a non-empty result means the projection was written by something other than the trigger — the
     * single most important invariant to alert on in production.
     */
    @Query(value = """
            select a.id
              from accounts a
              left join (
                   select account_id,
                          sum(case when direction = 'DEBIT'  then amount else 0 end) as debits,
                          sum(case when direction = 'CREDIT' then amount else 0 end) as credits
                     from journal_entries
                    group by account_id
              ) j on j.account_id = a.id
             where a.total_debits  <> coalesce(j.debits, 0)
                or a.total_credits <> coalesce(j.credits, 0)
            """, nativeQuery = true)
    java.util.List<UUID> findAccountsWithDriftedBalanceProjection();
}
