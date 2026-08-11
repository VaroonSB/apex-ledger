package com.apex.ledger.domain.model;

/**
 * Lifecycle state of an account.
 *
 * <p>{@link #postingAllowed()} is mirrored by the database: the balance trigger
 * {@code apex_apply_entry_to_account_balance()} only updates rows with {@code status = 'ACTIVE'} and
 * raises otherwise, so a posting to a frozen or closed account fails even if the application check
 * is bypassed.
 */
public enum AccountStatus {

    /** Open for posting. */
    ACTIVE(true),

    /** Temporarily blocked, e.g. under compliance review. Reversible. */
    FROZEN(false),

    /** Permanently retired. Terminal: history is retained, no further postings. */
    CLOSED(false);

    private final boolean postingAllowed;

    AccountStatus(boolean postingAllowed) {
        this.postingAllowed = postingAllowed;
    }

    public boolean postingAllowed() {
        return postingAllowed;
    }

    public boolean isTerminal() {
        return this == CLOSED;
    }
}
