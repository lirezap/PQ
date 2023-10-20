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
 * Different deferrable modes; used in transaction blocks.
 * This transaction property has no effect unless the transaction is also SERIALIZABLE and READ ONLY.
 *
 * @author Alireza Pourtaghi
 */
public enum DeferrableMode {
    /**
     * None; results in default one.
     */
    NONE(null),

    /**
     * When all three of these properties (SERIALIZABLE, READ ONLY and DEFERRABLE) are selected for a transaction, the
     * transaction may block when first acquiring its snapshot, after which it is able to run without the normal
     * overhead of a SERIALIZABLE transaction and without any risk of contributing to or being canceled by a
     * serialization failure. This mode is well suited for long-running reports or backups.
     */
    DEFERRABLE("DEFERRABLE"),

    /**
     * This is the default.
     */
    NOT_DEFERRABLE("NOT DEFERRABLE");

    private final String value;

    DeferrableMode(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
