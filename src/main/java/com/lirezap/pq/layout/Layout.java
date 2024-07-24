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

package com.lirezap.pq.layout;

import java.lang.foreign.MemoryLayout;
import java.lang.invoke.VarHandle;

/**
 * Memory layout accessor abstraction.
 *
 * @author Alireza Pourtaghi
 */
public abstract class Layout {

    /**
     * Returns underlying {@link MemoryLayout} related to this abstraction.
     *
     * @return {@link MemoryLayout} instance
     */
    public abstract MemoryLayout layout();

    /**
     * Returns a var handle that points to a field's value of underlying memory layout.
     *
     * @param name variable name
     * @return {@link VarHandle} instance
     */
    public abstract VarHandle var(
            String name);

    /**
     * Returns a var handle that points to an array of values for a specific field of underlying memory layout.
     *
     * @param name variable name
     * @return {@link VarHandle} instance
     */
    public abstract VarHandle arrayElementVar(
            String name);
}
