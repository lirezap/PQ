/*
 * ISC License
 *
 * Copyright (c) 2025, Alireza Pourtaghi <lirezap@protonmail.com>
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
 * Different isolation levels; used in transaction blocks.
 * The isolation level of a transaction determines what data the transaction can see when other transactions are
 * running concurrently
 *
 * @author Alireza Pourtaghi
 */
public enum IsolationLevel {
    /**
     * None; results in default one.
     */
    NONE(null),

    /**
     * All statements of the current transaction can only see rows committed before the first query or data-modification
     * statement was executed in this transaction. If a pattern of reads and writes among concurrent serializable
     * transactions would create a situation which could not have occurred for any serial (one-at-a-time) execution of
     * those transactions, one of them will be rolled back with a serialization_failure error.
     */
    SERIALIZABLE("ISOLATION LEVEL SERIALIZABLE"),

    /**
     * All statements of the current transaction can only see rows committed before the first query or data-modification
     * statement was executed in this transaction.
     */
    REPEATABLE_READ("ISOLATION LEVEL REPEATABLE READ"),

    /**
     * A statement can only see rows committed before it began. This is the default.
     */
    READ_COMMITTED("ISOLATION LEVEL READ COMMITTED"),

    /**
     * In postgresql READ UNCOMMITTED is treated as READ COMMITTED.
     */
    READ_UNCOMMITTED("ISOLATION LEVEL READ UNCOMMITTED");

    private final String value;

    IsolationLevel(final String value) {
        this.value = value;
    }

    public final String getValue() {
        return value;
    }
}
