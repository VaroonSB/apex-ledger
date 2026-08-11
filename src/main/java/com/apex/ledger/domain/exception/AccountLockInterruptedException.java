package com.apex.ledger.domain.exception;

import java.util.List;
import java.util.UUID;

/**
 * Raised when the thread waiting for an account lock was interrupted.
 *
 * <p>In this application that means shutdown: graceful shutdown interrupts in-flight request threads,
 * and a virtual thread parked waiting for a Redis lock is exactly the kind of work that gets cut
 * short. Nothing was written.
 *
 * <p>The interrupt status is restored before this is thrown, so the surrounding machinery can still
 * observe cancellation. Swallowing it would leave a thread that ignores the shutdown request.
 */
public final class AccountLockInterruptedException extends LedgerException {

    private static final long serialVersionUID = 1L;

    private final List<UUID> accountIds;

    public AccountLockInterruptedException(List<UUID> accountIds, InterruptedException cause) {
        super(("interrupted while waiting for the distributed lock on account(s) %s; "
                + "nothing was written").formatted(accountIds), cause);
        this.accountIds = List.copyOf(accountIds);
    }

    public List<UUID> accountIds() {
        return accountIds;
    }

    @Override
    public String errorCode() {
        return "ACCOUNT_LOCK_INTERRUPTED";
    }
}
