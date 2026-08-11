package com.apex.ledger.application.service;

import com.apex.ledger.api.graphql.dto.AccountBalanceView;
import com.apex.ledger.api.graphql.dto.JournalEntryView;
import com.apex.ledger.api.graphql.dto.TransactionView;
import com.apex.ledger.application.port.out.AccountBalanceProjection;
import com.apex.ledger.infrastructure.persistence.entity.Account;
import com.apex.ledger.infrastructure.persistence.entity.JournalEntry;
import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import com.apex.ledger.infrastructure.persistence.repository.JournalEntryRepository;
import com.apex.ledger.infrastructure.persistence.repository.TransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The read side of the ledger.
 *
 * <p>Separate from {@link LedgerEngineService} because reads and writes have almost nothing in common
 * here. Writes need a distributed lock, a transaction and an outbox row; reads need none of that, and
 * bundling them would put a query behind machinery it does not use. It also keeps the GraphQL
 * controller out of the repositories, so the API layer depends on the application layer rather than
 * reaching past it into persistence.
 *
 * <p>Every method is {@code readOnly}. That is not decoration: it lets Hibernate skip dirty-checking
 * and flushing, and it means a read can be routed to a replica without any code change.
 */
@Service
@Transactional(readOnly = true)
public class LedgerQueryService {

    /** Default statement page size when the client does not ask for one. */
    public static final int DEFAULT_PAGE_SIZE = 20;

    /**
     * Hard ceiling on a page.
     *
     * <p>Not negotiable by the client. Without it, {@code first: 1000000} is a denial-of-service
     * primitive against an append-only table that only ever grows — one request could pull an
     * account's entire history through the JVM heap and the serializer.
     */
    public static final int MAX_PAGE_SIZE = 100;

    private final AccountRepository accounts;
    private final TransactionRepository transactions;
    private final JournalEntryRepository journalEntries;
    private final AccountBalanceProjection balanceProjection;
    private final Clock clock;

    public LedgerQueryService(AccountRepository accounts,
                             TransactionRepository transactions,
                             JournalEntryRepository journalEntries,
                             AccountBalanceProjection balanceProjection,
                             Clock clock) {
        this.accounts = Objects.requireNonNull(accounts);
        this.transactions = Objects.requireNonNull(transactions);
        this.journalEntries = Objects.requireNonNull(journalEntries);
        this.balanceProjection = Objects.requireNonNull(balanceProjection);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * An account's balance and projection counters.
     *
     * <p>Reads the account row rather than only the cached balance, because the GraphQL type also
     * exposes the account number, type, status and lifetime totals — a cache hit that returned just the
     * balance would still need this row. The cache is consulted for the balance itself so a warm entry
     * still avoids recomputing anything, and {@code asOf} tells the client how current the answer is.
     */
    public Optional<AccountBalanceView> findAccountBalance(UUID accountId) {
        Objects.requireNonNull(accountId, "accountId must not be null");
        return accounts.findById(accountId)
                .map(account -> AccountBalanceView.from(account, clock.instant()));
    }

    /**
     * Balances for several accounts, preserving the requested order.
     *
     * <p>Used to fill {@code PostTransactionPayload.balancesAfter} without a query per account.
     */
    public List<AccountBalanceView> findAccountBalances(Collection<UUID> accountIds) {
        Objects.requireNonNull(accountIds, "accountIds must not be null");
        Instant asOf = clock.instant();
        return accountIds.stream()
                .map(accounts::findById)
                .flatMap(Optional::stream)
                .map(account -> AccountBalanceView.from(account, asOf))
                .toList();
    }

    /** Warms or reads the cached balance directly, bypassing the account row. */
    public Optional<java.math.BigDecimal> findCachedBalance(UUID accountId) {
        return balanceProjection.findBalance(accountId).map(money -> money.amount());
    }

    /**
     * One page of an account's statement, newest first.
     *
     * <p>Fetches {@code limit + 1} rows and returns at most {@code limit}. The extra row is how
     * {@code hasNextPage} is answered without a second {@code COUNT} query — if it comes back, there is
     * more; if it does not, this is the last page. On an unbounded journal that saves a scan on every
     * single page request.
     *
     * @param afterCreatedAt cursor timestamp, or {@code null} for the first page
     * @param afterId cursor id, required when {@code afterCreatedAt} is given
     */
    public StatementPage findAccountStatement(UUID accountId, int requestedSize,
                                              Instant afterCreatedAt, UUID afterId) {
        Objects.requireNonNull(accountId, "accountId must not be null");
        int limit = normalisePageSize(requestedSize);
        int probe = limit + 1;

        List<JournalEntry> rows = (afterCreatedAt == null)
                ? journalEntries.findFirstPageByAccountId(accountId, probe)
                : journalEntries.findPageByAccountIdAfter(accountId,
                        afterCreatedAt,
                        Objects.requireNonNull(afterId, "afterId is required with afterCreatedAt"),
                        probe);

        boolean hasNext = rows.size() > limit;
        List<JournalEntryView> page = rows.stream()
                .limit(limit)
                .map(JournalEntryView::from)
                .toList();
        return new StatementPage(page, hasNext);
    }

    /** Clamps a client-supplied page size into the permitted range. */
    public static int normalisePageSize(int requestedSize) {
        if (requestedSize <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(requestedSize, MAX_PAGE_SIZE);
    }

    public Optional<TransactionView> findTransaction(UUID transactionId) {
        Objects.requireNonNull(transactionId, "transactionId must not be null");
        return transactions.findById(transactionId).map(TransactionView::from);
    }

    /**
     * Transactions by id, keyed for a GraphQL batch loader.
     *
     * <p>This is the N+1 fix for {@code JournalEntry.transaction}: one query for a whole page of
     * statement lines instead of one per line.
     */
    public Map<UUID, TransactionView> findTransactionsByIds(Collection<UUID> transactionIds) {
        Objects.requireNonNull(transactionIds, "transactionIds must not be null");
        if (transactionIds.isEmpty()) {
            return Map.of();
        }
        return transactions.findByIdIn(transactionIds).stream()
                .collect(Collectors.toMap(
                        transaction -> transaction.getId(),
                        TransactionView::from,
                        (first, second) -> first,
                        LinkedHashMap::new));
    }

    /**
     * Entries grouped by transaction, keyed for a GraphQL batch loader.
     *
     * <p>The N+1 fix for {@code Transaction.entries}. Returns an empty list for a transaction with no
     * entries rather than omitting the key, so the loader never resolves the field to null — the schema
     * declares it non-nullable, and a missing key would surface as a confusing execution error rather
     * than an empty statement.
     */
    public Map<UUID, List<JournalEntryView>> findEntriesByTransactionIds(
            Collection<UUID> transactionIds) {
        Objects.requireNonNull(transactionIds, "transactionIds must not be null");
        if (transactionIds.isEmpty()) {
            return Map.of();
        }
        Map<UUID, List<JournalEntryView>> grouped = new LinkedHashMap<>();
        for (UUID transactionId : transactionIds) {
            grouped.put(transactionId, new java.util.ArrayList<>());
        }
        journalEntries
                .findByTransactionIdInOrderByTransactionIdAscEntrySequenceAsc(transactionIds)
                .forEach(entry -> grouped
                        .computeIfAbsent(entry.getTransactionId(), key -> new java.util.ArrayList<>())
                        .add(JournalEntryView.from(entry)));
        return grouped.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue()),
                        (first, second) -> first,
                        LinkedHashMap::new));
    }

    /** Whether an account exists, without loading it. */
    public boolean accountExists(UUID accountId) {
        return accounts.findById(accountId).isPresent();
    }

    /** A page of statement lines plus whether more follow. */
    public record StatementPage(List<JournalEntryView> entries, boolean hasNextPage) {
        public StatementPage {
            entries = List.copyOf(entries);
        }
    }

    /** The {@link Account} rows behind a set of ids, for callers that need the entities. */
    List<Account> loadAccounts(Collection<UUID> accountIds) {
        return accountIds.stream().map(accounts::findById).flatMap(Optional::stream).toList();
    }
}
