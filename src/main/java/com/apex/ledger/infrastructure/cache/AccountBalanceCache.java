package com.apex.ledger.infrastructure.cache;

import com.apex.ledger.application.port.out.AccountBalanceProjection;
import com.apex.ledger.config.ApexLedgerProperties;
import com.apex.ledger.domain.model.CurrencyCode;
import com.apex.ledger.domain.model.Money;
import com.apex.ledger.infrastructure.persistence.entity.Account;
import com.apex.ledger.infrastructure.persistence.repository.AccountRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-through / write-through cache for account balances, on Spring Data Redis.
 *
 * <p>Its job is to keep balance inquiries — the highest-volume, least-critical query in the system —
 * away from PostgreSQL, so the database's capacity is spent on postings.
 *
 * <h2>Storage layout</h2>
 *
 * <p>A Redis hash per account, at {@code apex:balance:{accountId}}, with plain string fields:
 * {@code currency}, {@code balance}, {@code fence}. Deliberately <em>not</em> a serialised Java object:
 *
 * <ul>
 *   <li>{@code GenericJackson2JsonRedisSerializer} embeds {@code @class} type information, which turns
 *       any write access to Redis into a deserialization gadget. Strings cannot be exploited that way.
 *   <li>{@code BigDecimal} survives as its exact {@code toPlainString()} form. Routing money through
 *       a JSON number risks a {@code double} on either end of the trip.
 *   <li>The fence is readable by a Lua script as its own field, without decoding a payload.
 * </ul>
 *
 * <h2>Write ordering: the fence</h2>
 *
 * <p>The subtle failure of any write-through balance cache is an <em>out-of-order</em> write. Two
 * postings commit in order, but their cache writes race, and the older balance lands last — leaving a
 * wrong balance that persists until the TTL expires. A read-through population racing a write-through
 * has the same shape.
 *
 * <p>So every write carries a fence: {@code total_debits + total_credits}, which is monotonically
 * non-decreasing because both totals only ever grow (guaranteed by
 * {@code ck_accounts_totals_non_negative} and the append-only journal). A Lua script compares the
 * incoming fence with the stored one and discards the write if it is older, making the cache
 * eventually consistent in the right direction under any interleaving.
 *
 * <p>The comparison happens on a fixed-width zero-padded encoding rather than numerically, because Lua
 * numbers are IEEE doubles and would lose precision on a {@code NUMERIC(38,18)} value — silently
 * comparing two distinct fences as equal. Zero-padding to a fixed width makes lexicographic string
 * comparison exactly equivalent to numeric comparison for non-negative decimals, which Lua does
 * precisely.
 *
 * <h2>Failure policy: never fail a read because Redis is unwell</h2>
 *
 * <p>Every Redis interaction is wrapped so that a Redis outage degrades this class to a pass-through to
 * PostgreSQL. A cache that takes the system down when it fails is a liability, and the whole point of
 * the fence and the TTL is that a missed write is self-correcting.
 */
@Component
public class AccountBalanceCache implements AccountBalanceProjection {

    private static final Logger log = LoggerFactory.getLogger(AccountBalanceCache.class);

    private static final String FIELD_CURRENCY = "currency";
    private static final String FIELD_BALANCE = "balance";
    private static final String FIELD_FENCE = "fence";

    /**
     * Integer digits available in {@code NUMERIC(38,18)}: 38 - 18 = 20. The fence is the sum of two
     * such values, so it needs one extra digit of headroom to be safe against a carry.
     */
    private static final int FENCE_INTEGER_DIGITS = 21;
    private static final int FENCE_SCALE = 18;

    /**
     * Compare-and-set write. Replaces the cached balance only when the incoming fence is at least as
     * current as the stored one, then refreshes the TTL.
     *
     * <p>Returns 1 when the write was applied and 0 when it was rejected as stale. Runs as a single
     * atomic Redis operation, so no concurrent writer can interleave between the read and the write.
     */
    private static final RedisScript<Long> WRITE_THROUGH_SCRIPT = new DefaultRedisScript<>("""
            local existingFence = redis.call('HGET', KEYS[1], 'fence')
            if existingFence and existingFence > ARGV[3] then
              return 0
            end
            redis.call('HSET', KEYS[1], 'currency', ARGV[1], 'balance', ARGV[2], 'fence', ARGV[3])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final AccountRepository accounts;
    private final boolean enabled;
    private final String keyPrefix;
    private final Duration ttl;

    private final Counter hits;
    private final Counter misses;
    private final Counter staleWritesRejected;
    private final Counter redisFailures;

    public AccountBalanceCache(StringRedisTemplate redis,
                               AccountRepository accounts,
                               ApexLedgerProperties properties,
                               MeterRegistry meterRegistry) {
        this.redis = Objects.requireNonNull(redis, "redis must not be null");
        this.accounts = Objects.requireNonNull(accounts, "accounts must not be null");
        this.enabled = properties.cache().enabled();
        this.keyPrefix = properties.cache().keyPrefix();
        this.ttl = properties.cache().ttl();

        this.hits = Counter.builder("apex.ledger.balance.cache")
                .tag("result", "hit").register(meterRegistry);
        this.misses = Counter.builder("apex.ledger.balance.cache")
                .tag("result", "miss").register(meterRegistry);
        this.staleWritesRejected = Counter.builder("apex.ledger.balance.cache.stale.rejected")
                .description("Write-through attempts discarded because a newer balance was already "
                        + "cached; a non-zero rate means commits and cache writes are racing")
                .register(meterRegistry);
        this.redisFailures = Counter.builder("apex.ledger.balance.cache.redis.failures")
                .description("Redis operations that failed and fell back to PostgreSQL")
                .register(meterRegistry);
    }

    // ------------------------------------------------------------------ read

    @Override
    public Optional<Money> findBalance(UUID accountId) {
        Objects.requireNonNull(accountId, "accountId must not be null");

        if (enabled) {
            Optional<Money> cached = readFromCache(accountId);
            if (cached.isPresent()) {
                hits.increment();
                return cached;
            }
            misses.increment();
        }
        return readThrough(accountId);
    }

    private Optional<Money> readFromCache(UUID accountId) {
        try {
            List<Object> values = redis.opsForHash()
                    .multiGet(key(accountId), List.of(FIELD_CURRENCY, FIELD_BALANCE));
            Object currency = values.get(0);
            Object balance = values.get(1);
            if (currency == null || balance == null) {
                return Optional.empty();
            }
            return Optional.of(Money.of(
                    new BigDecimal(balance.toString()), CurrencyCode.of(currency.toString())));
        } catch (DataAccessException | IllegalArgumentException | ArithmeticException e) {
            // IllegalArgumentException/ArithmeticException here mean a corrupt or stale-format entry.
            // Drop it rather than serve it, and let the read fall through to PostgreSQL.
            redisFailures.increment();
            log.warn("balance cache read failed for account {}; falling back to PostgreSQL",
                    accountId, e);
            evictQuietly(accountId);
            return Optional.empty();
        }
    }

    /**
     * Populates the cache from PostgreSQL.
     *
     * <p>Goes through the same fenced write as a post-commit update, so a population that raced a
     * newer commit cannot install an older balance.
     */
    private Optional<Money> readThrough(UUID accountId) {
        Optional<Account> loaded = accounts.findById(accountId);
        if (loaded.isEmpty()) {
            return Optional.empty();
        }
        Account account = loaded.get();
        Money balance = account.getBalance();
        if (enabled) {
            write(accountId, balance, fenceOf(account));
        }
        return Optional.of(balance);
    }

    /** The monotonic fence for an account: the sum of its two lifetime totals. */
    public static BigDecimal fenceOf(Account account) {
        return account.getTotalDebits().amount().add(account.getTotalCredits().amount());
    }

    // ----------------------------------------------------------------- write

    @Override
    public void recordCommittedBalance(UUID accountId, Money balance, BigDecimal fence) {
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(balance, "balance must not be null");
        Objects.requireNonNull(fence, "fence must not be null");
        if (!enabled) {
            return;
        }
        write(accountId, balance, fence);
    }

    private void write(UUID accountId, Money balance, BigDecimal fence) {
        try {
            Long applied = redis.execute(
                    WRITE_THROUGH_SCRIPT,
                    List.of(key(accountId)),
                    balance.currency().code(),
                    balance.amount().toPlainString(),
                    encodeFence(fence),
                    Long.toString(ttl.toMillis()));
            if (applied != null && applied == 0L) {
                staleWritesRejected.increment();
                log.debug("discarded stale balance write for account {} (fence {})",
                        accountId, fence.toPlainString());
            }
        } catch (DataAccessException e) {
            // A lost write is bounded by the TTL and corrected by the next read-through.
            redisFailures.increment();
            log.warn("balance cache write failed for account {}; the entry will be refreshed on the "
                    + "next read or when the TTL expires", accountId, e);
        }
    }

    @Override
    public void evict(UUID accountId) {
        Objects.requireNonNull(accountId, "accountId must not be null");
        if (!enabled) {
            return;
        }
        evictQuietly(accountId);
    }

    private void evictQuietly(UUID accountId) {
        try {
            redis.delete(key(accountId));
        } catch (DataAccessException e) {
            redisFailures.increment();
            log.warn("balance cache eviction failed for account {}; the stale entry will expire "
                    + "after the TTL", accountId, e);
        }
    }

    // ------------------------------------------------------------- internals

    private String key(UUID accountId) {
        return keyPrefix + accountId;
    }

    /**
     * Encodes a non-negative fence as a fixed-width, zero-padded digit string, so that lexicographic
     * comparison matches numeric comparison exactly.
     *
     * <p>For example with a 21/18 layout, {@code 100.5} becomes
     * {@code "000000000000000000100" + "500000000000000000"}. Comparing two such strings byte by byte
     * gives the same answer as comparing the numbers, which is what lets the Lua script order fences
     * without touching Lua's double arithmetic.
     */
    static String encodeFence(BigDecimal fence) {
        if (fence.signum() < 0) {
            throw new IllegalArgumentException(
                    ("fence must be non-negative, got %s; it is the sum of two monotonically "
                            + "increasing lifetime totals and can never decrease")
                            .formatted(fence.toPlainString()));
        }
        BigDecimal scaled = fence.setScale(FENCE_SCALE, java.math.RoundingMode.UNNECESSARY);
        String unscaledDigits = scaled.unscaledValue().toString();
        int totalWidth = FENCE_INTEGER_DIGITS + FENCE_SCALE;
        if (unscaledDigits.length() > totalWidth) {
            throw new IllegalArgumentException(
                    ("fence %s exceeds the %d digits reserved for the cache fence")
                            .formatted(fence.toPlainString(), totalWidth));
        }
        return "0".repeat(totalWidth - unscaledDigits.length()) + unscaledDigits;
    }

    /** Exposed for diagnostics: the raw cached entry, without a read-through on miss. */
    public Optional<Map<Object, Object>> rawEntry(UUID accountId) {
        try {
            Map<Object, Object> entry = redis.opsForHash().entries(key(accountId));
            return entry.isEmpty() ? Optional.empty() : Optional.of(entry);
        } catch (DataAccessException e) {
            redisFailures.increment();
            return Optional.empty();
        }
    }
}
