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

package software.openex.pq.cp.xact;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transaction block abstraction; that includes conn in transaction, pool index and done status handle.
 *
 * @author Alireza pourtaghi
 */
public final class TransactionBlock {
    private final int index;
    private final MemorySegment conn;
    private final AtomicBoolean done;

    public TransactionBlock(final int index, final MemorySegment conn) {
        this.index = index;
        this.conn = conn;
        this.done = new AtomicBoolean(false);
    }

    public int getIndex() {
        return index;
    }

    public MemorySegment getConn() {
        return conn;
    }

    public AtomicBoolean getDone() {
        return done;
    }
}
