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

package ir.jibit.pq.cp.tx;

/**
 * Different isolation levels; used in transaction blocks.
 *
 * @author Alireza Pourtaghi
 */
public enum IsolationLevel {
    NONE(null),
    SERIALIZABLE("ISOLATION LEVEL SERIALIZABLE"),
    REPEATABLE_READ("ISOLATION LEVEL REPEATABLE READ"),
    READ_COMMITTED("ISOLATION LEVEL READ COMMITTED"),
    READ_UNCOMMITTED("ISOLATION LEVEL READ UNCOMMITTED");

    private final String value;

    IsolationLevel(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
