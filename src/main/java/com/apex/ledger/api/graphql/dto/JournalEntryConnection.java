package com.apex.ledger.api.graphql.dto;

import java.util.List;

/**
 * A Relay-shaped page of journal entries.
 *
 * <p>{@code hasPreviousPage} reports whether the caller supplied a cursor, which is the honest answer
 * for a forward-only connection: determining it properly would mean an extra backwards seek to prove
 * rows exist before the window, and nothing in this API pages backwards. Relay permits this — the
 * specification only requires the field to be accurate when paginating in that direction.
 */
public record JournalEntryConnection(
        List<JournalEntryEdge> edges,
        PageInfoView pageInfo
) {
    public JournalEntryConnection {
        edges = List.copyOf(edges);
    }

    /** An edge pairs a node with the opaque cursor that names its position. */
    public record JournalEntryEdge(JournalEntryView node, String cursor) {
    }

    public record PageInfoView(
            boolean hasNextPage,
            boolean hasPreviousPage,
            String startCursor,
            String endCursor
    ) {
    }
}
