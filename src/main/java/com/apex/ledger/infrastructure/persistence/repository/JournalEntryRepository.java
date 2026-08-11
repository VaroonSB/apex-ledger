package com.apex.ledger.infrastructure.persistence.repository;

import com.apex.ledger.infrastructure.persistence.entity.JournalEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence access for the append-only {@link JournalEntry} table.
 *
 * <p>Insert and read only — the sixth layer of the immutability guarantee described on
 * {@link JournalEntry}. {@code JpaRepository} would expose {@code delete}, {@code deleteAll} and
 * {@code deleteAllInBatch} on the audit record of the entire system; every one of them would be
 * rejected by {@code trg_journal_entries_append_only} at runtime, so they are simply not declared.
 */
public interface JournalEntryRepository extends Repository<JournalEntry, UUID> {

    JournalEntry save(JournalEntry entry);

    /**
     * Inserts all postings of a transaction.
     *
     * <p>Preferred over repeated {@code save}: the entries of one transaction are written together, and
     * batching them is what makes {@code reWriteBatchedInserts} and {@code hibernate.jdbc.batch_size}
     * from {@code application.yml} effective. The deferred balance trigger is what permits this — it
     * checks the entry set at COMMIT rather than after each row, so a batch is never transiently
     * "unbalanced".
     */
    List<JournalEntry> saveAll(Iterable<JournalEntry> entries);

    Optional<JournalEntry> findById(UUID id);

    /** All postings of one transaction, in deterministic order. */
    List<JournalEntry> findByTransactionIdOrderByEntrySequenceAsc(UUID transactionId);

    /** Statement history for an account, newest first. Served by {@code idx_journal_entries_account_created}. */
    Page<JournalEntry> findByAccountIdOrderByCreatedAtDescIdDesc(UUID accountId, Pageable pageable);

    long countByTransactionId(UUID transactionId);

    /**
     * Balance for an account computed from the journal itself, signed by direction.
     *
     * <p>The authoritative figure, as opposed to the {@code accounts} projection. O(entries) for the
     * account, so this is for reconciliation and audit, not for the posting hot path.
     */
    @Query(value = """
            select coalesce(sum(case when direction = 'DEBIT' then amount else -amount end), 0)
              from journal_entries
             where account_id = :accountId
            """, nativeQuery = true)
    BigDecimal sumSignedAmountByAccountId(@Param("accountId") UUID accountId);

    /**
     * Global double-entry check: the signed sum of every posting in every currency.
     *
     * <p>Must be exactly zero. Any other result means the ledger as a whole no longer balances, which
     * is the strongest single alarm this system can raise.
     */
    @Query(value = """
            select currency,
                   coalesce(sum(case when direction = 'DEBIT' then amount else -amount end), 0)
              from journal_entries
             group by currency
            having coalesce(sum(case when direction = 'DEBIT' then amount else -amount end), 0) <> 0
            """, nativeQuery = true)
    List<Object[]> findCurrenciesThatDoNotBalance();
}
