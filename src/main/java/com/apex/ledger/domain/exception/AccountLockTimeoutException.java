package com.apex.ledger.domain.exception;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Raised when the distributed lock for the accounts of a posting could not be acquired within the
 * configured wait time.
 *
 * <p>This is a <strong>load-shedding signal, not a correctness failure</strong>. Nothing was written;
 * the accounts are simply busy. The caller may retry, ideally with jitter — which is why
 * {@link #isRetryable()} is unconditionally true and the API layer should map this to a 503/429-shaped
 * response rather than a 4xx business error.
 *
 * <p>Timing out is the intended behaviour under contention. The alternative — waiting indefinitely —
 * would let requests for one hot account accumulate until they exhaust the JDBC pool, converting
 * contention on a single account into an outage for every account.
 */
public final class AccountLockTimeoutException extends LedgerException {

    private static final long serialVersionUID = 1L;

    private final List<UUID> accountIds;
    private final Duration waitTime;

    public AccountLockTimeoutException(List<UUID> accountIds, Duration waitTime) {
        super(("could not acquire the distributed lock for account(s) %s within %s; "
                + "the accounts are contended and nothing was written — retry with backoff")
                .formatted(accountIds, waitTime));
        this.accountIds = List.copyOf(accountIds);
        this.waitTime = waitTime;
    }

    /** The accounts whose lock set could not be acquired, in the order locking was attempted. */
    public List<UUID> accountIds() {
        return accountIds;
    }

    public Duration waitTime() {
        return waitTime;
    }

    /** Always true: no state changed, so a retry is safe and may succeed. */
    public boolean isRetryable() {
        return true;
    }

    @Override
    public String errorCode() {
        return "ACCOUNT_LOCK_TIMEOUT";
    }
}
