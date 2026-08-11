package com.apex.ledger.api.graphql.controller;

import com.apex.ledger.api.graphql.dto.AccountBalanceView;
import com.apex.ledger.api.graphql.dto.JournalEntryConnection;
import com.apex.ledger.api.graphql.dto.JournalEntryView;
import com.apex.ledger.api.graphql.dto.PostTransactionInput;
import com.apex.ledger.api.graphql.dto.PostTransactionPayload;
import com.apex.ledger.api.graphql.dto.TransactionView;
import com.apex.ledger.api.graphql.support.EntryCursor;
import com.apex.ledger.application.port.in.PostTransferCommand;
import com.apex.ledger.application.port.in.PostTransferResult;
import com.apex.ledger.application.service.LedgerEngineService;
import com.apex.ledger.application.service.LedgerQueryService;
import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.Direction;
import com.apex.ledger.domain.model.IdempotencyKey;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.config.ResilienceConfig;
import com.apex.ledger.domain.model.TransactionKind;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.BatchMapping;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The GraphQL API surface.
 *
 * <p>Deliberately thin. It translates between the wire contract and the application layer and does
 * nothing else: no validation beyond shape, no orchestration, no transaction. Every ledger rule lives
 * in {@link LedgerEngineService} and, beneath it, in database constraints — so an alternative transport
 * (gRPC, a batch importer) enforces exactly the same rules without re-implementing anything.
 *
 * <p>There is no {@code @Transactional} here on purpose. A transaction spanning a GraphQL execution
 * would stay open across field resolution and serialisation, holding a pooled JDBC connection for the
 * whole response. The engine opens its own transaction around the write; reads run in the query
 * service's read-only transactions.
 */
@Controller
@Validated
public class LedgerGraphqlController {

    private static final Logger log = LoggerFactory.getLogger(LedgerGraphqlController.class);

    /**
     * Recorded against every transaction this API creates.
     *
     * <p>A placeholder for a real principal, and named so it is obviously not one. Authentication is
     * not wired in this phase; once it is, this becomes the authenticated subject. It is deliberately
     * not something innocuous like "system", which would be indistinguishable from a legitimate
     * internal actor in an audit trail.
     */
    private static final String UNAUTHENTICATED_PRINCIPAL = "graphql-api:unauthenticated";

    private final LedgerEngineService engine;
    private final LedgerQueryService queries;
    private final Clock clock;

    public LedgerGraphqlController(LedgerEngineService engine,
                                   LedgerQueryService queries,
                                   Clock clock) {
        this.engine = Objects.requireNonNull(engine);
        this.queries = Objects.requireNonNull(queries);
        this.clock = Objects.requireNonNull(clock);
    }

    // ------------------------------------------------------------------ queries

    @QueryMapping
    public String ping() {
        return "pong";
    }

    /**
     * {@code Query.getAccountBalance}. Returns {@code null} for an unknown account.
     *
     * <p>Null rather than an error: asking about an account that does not exist is a legitimate query
     * with an empty answer, and the schema types the field as nullable to say so. Raising NOT_FOUND
     * would force clients to treat a normal outcome as an exception.
     */
    @QueryMapping
    public AccountBalanceView getAccountBalance(@Argument UUID accountId) {
        return queries.findAccountBalance(accountId).orElse(null);
    }

    /**
     * {@code Query.getTransactionHistory} — cursor-paginated statement, newest first.
     *
     * <p>The cursor is decoded here, in the transport layer, and passed on as typed
     * {@code (Instant, UUID)} values. The query service therefore knows nothing about cursor encoding,
     * which means the wire format can change without touching the read model.
     */
    @QueryMapping
    public JournalEntryConnection getTransactionHistory(@Argument UUID accountId,
                                                        @Argument Integer first,
                                                        @Argument String after) {
        int pageSize = LedgerQueryService.normalisePageSize(first == null ? 0 : first);

        Instant afterCreatedAt = null;
        UUID afterId = null;
        if (after != null && !after.isBlank()) {
            EntryCursor cursor = EntryCursor.decode(after);
            afterCreatedAt = cursor.createdAt();
            afterId = cursor.id();
        }

        LedgerQueryService.StatementPage page =
                queries.findAccountStatement(accountId, pageSize, afterCreatedAt, afterId);

        List<JournalEntryConnection.JournalEntryEdge> edges = page.entries().stream()
                .map(entry -> new JournalEntryConnection.JournalEntryEdge(
                        entry, EntryCursor.of(entry).encode()))
                .toList();

        String startCursor = edges.isEmpty() ? null : edges.get(0).cursor();
        String endCursor = edges.isEmpty() ? null : edges.get(edges.size() - 1).cursor();

        return new JournalEntryConnection(
                edges,
                new JournalEntryConnection.PageInfoView(
                        page.hasNextPage(),
                        // Forward-only connection: the caller had a cursor, so something precedes this
                        // window. Proving it with a backwards seek would cost a query nothing reads.
                        after != null && !after.isBlank(),
                        startCursor,
                        endCursor));
    }

    @QueryMapping
    public TransactionView getTransaction(@Argument UUID transactionId) {
        return queries.findTransaction(transactionId).orElse(null);
    }

    // ---------------------------------------------------------------- mutations

    /**
     * {@code Mutation.postTransaction} — posts a balanced two-account transaction.
     *
     * <p>Translates source/destination into the engine's N-leg command. The convention, spelled out in
     * the schema: <b>source is CREDITED, destination is DEBITED</b>. Whether that raises or lowers a
     * given balance depends on the account type, because a debit increases an ASSET and decreases a
     * LIABILITY. No attempt is made to infer intent from account types — a rule like "always reduce the
     * source's balance" is not expressible in double-entry, since applied to a mixed asset/liability
     * pair it yields two credits, which do not sum to zero.
     *
     * <p>The engine remains N-leg; this mutation exposes the two-leg case only. FX through a position
     * account and fee splits need the multi-leg form, which this phase does not publish.
     *
     * <h2>Rate limiting</h2>
     *
     * <p>Guarded by a Resilience4j rate limiter, configured in {@link ResilienceConfig}. It sits on the
     * <em>outermost</em> layer on purpose: a shed request must be refused before it has taken a JDBC
     * connection, a distributed lock or a transaction, because the whole point is to stop a spike from
     * saturating the connection pool. A limiter placed inside the engine would already have paid those
     * costs by the time it rejected.
     *
     * <p>Rejection surfaces as {@code RequestNotPermitted}, which {@code LedgerExceptionResolver} maps to
     * a retryable {@code RATE_LIMITED} GraphQL error. Nothing is written, so a client may retry with
     * backoff — and reusing the same idempotency key is safe, since a rejected request never consumed it.
     *
     * <p>Only the mutation is limited. The read queries are cheap, cacheable and cannot exhaust the
     * pool the way a write can; throttling them would degrade dashboards during exactly the incident an
     * operator needs them for.
     */
    @RateLimiter(name = ResilienceConfig.POST_TRANSACTION_LIMITER)
    @MutationMapping
    public PostTransactionPayload postTransaction(@Argument @Valid PostTransactionInput input) {
        CurrencyCode currency = CurrencyCode.of(input.currency());
        Money amount = Money.of(input.amount(), currency);

        PostTransferCommand command = new PostTransferCommand(
                IdempotencyKey.of(input.idempotencyKey()),
                TransactionKind.TRANSFER,
                List.of(
                        new PostTransferCommand.Leg(
                                input.destinationAccountId(), Direction.DEBIT, amount),
                        new PostTransferCommand.Leg(
                                input.sourceAccountId(), Direction.CREDIT, amount)),
                input.reference(),
                input.description(),
                // Passed through as-is, INCLUDING null. The engine defaults it after fingerprinting;
                // defaulting it here would make every retry look like a different request.
                input.effectiveAt(),
                UNAUTHENTICATED_PRINCIPAL,
                null);

        PostTransferResult result = engine.post(command);

        TransactionView transaction = queries.findTransaction(result.transactionId())
                .orElseThrow(() -> new IllegalStateException(
                        "transaction %s was just committed but could not be read back"
                                .formatted(result.transactionId())));

        // A replay produced no new balances, so nothing is reported rather than reporting values that
        // are unrelated to this call.
        List<AccountBalanceView> balancesAfter = result.replayed()
                ? List.of()
                : queries.findAccountBalances(orderedAccounts(input));

        log.debug("postTransaction {} -> transaction {} (replayed={})",
                input.idempotencyKey(), result.transactionId(), result.replayed());

        return new PostTransactionPayload(transaction, result.replayed(), balancesAfter);
    }

    private Set<UUID> orderedAccounts(PostTransactionInput input) {
        Set<UUID> ordered = new LinkedHashSet<>();
        ordered.add(input.sourceAccountId());
        ordered.add(input.destinationAccountId());
        return ordered;
    }

    // ----------------------------------------------------------- batch loaders

    /**
     * Resolves {@code JournalEntry.transaction} for a whole page at once.
     *
     * <p>This is the N+1 fix. Without it, a 100-line statement that selects
     * {@code transaction { reference }} issues 100 queries — the single most common way a GraphQL API
     * quietly becomes the slowest part of a system. {@code @BatchMapping} collects the keys resolved in
     * one execution and calls this once.
     *
     * <p>The returned map must contain an entry for every input, or the non-nullable field resolves to
     * null and the whole query errors. A foreign key guarantees the transaction exists, so a missing key
     * here would be corruption rather than an ordinary absence.
     */
    @BatchMapping(typeName = "JournalEntry", field = "transaction")
    public Map<JournalEntryView, TransactionView> transaction(List<JournalEntryView> entries) {
        Set<UUID> transactionIds = entries.stream()
                .map(JournalEntryView::transactionId)
                .collect(LinkedHashSet::new, Set::add, Set::addAll);

        Map<UUID, TransactionView> byId = queries.findTransactionsByIds(transactionIds);

        return entries.stream()
                .filter(entry -> byId.containsKey(entry.transactionId()))
                .collect(java.util.stream.Collectors.toMap(
                        entry -> entry,
                        entry -> byId.get(entry.transactionId()),
                        (first, second) -> first,
                        java.util.LinkedHashMap::new));
    }

    /**
     * Resolves {@code Transaction.entries} for a whole page at once.
     *
     * <p>Kept off {@link TransactionView} as a field so a query that does not ask for entries never
     * loads them.
     */
    @BatchMapping(typeName = "Transaction", field = "entries")
    public Map<TransactionView, List<JournalEntryView>> entries(List<TransactionView> transactions) {
        Set<UUID> ids = transactions.stream()
                .map(TransactionView::id)
                .collect(LinkedHashSet::new, Set::add, Set::addAll);

        Map<UUID, List<JournalEntryView>> byTransaction = queries.findEntriesByTransactionIds(ids);

        return transactions.stream()
                .collect(java.util.stream.Collectors.toMap(
                        transaction -> transaction,
                        transaction -> byTransaction.getOrDefault(transaction.id(), List.of()),
                        (first, second) -> first,
                        java.util.LinkedHashMap::new));
    }
}
