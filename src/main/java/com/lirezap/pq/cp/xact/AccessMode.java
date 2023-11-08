/*
 * ISC License
 *
 * Copyright (c) 2023, Alireza Pourtaghi <lirezap@protonmail.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 */

package com.lirezap.pq.cp.xact;

/**
 * Different access modes; used in transaction blocks.
 * The transaction access mode determines whether the transaction is read/write or read-only.
 *
 * @author Alireza Pourtaghi
 */
public enum AccessMode {
    /**
     * None; results in default one.
     */
    NONE(null),

    /**
     * Read/write is the default.
     */
    READ_WRITE("READ WRITE"),

    /**
     * When a transaction is read-only, the following SQL commands are disallowed: INSERT, UPDATE, DELETE, MERGE, and
     * COPY FROM if the table they would write to is not a temporary table; all CREATE, ALTER, and DROP commands;
     * COMMENT, GRANT, REVOKE, TRUNCATE; and EXPLAIN ANALYZE and EXECUTE if the command they would execute is among
     * those listed. This is a high-level notion of read-only that does not prevent all writes to disk.
     */
    READ_ONLY("READ ONLY");

    private final String value;

    AccessMode(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
