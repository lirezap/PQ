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

package com.lirezap.pq.layouts;

import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.MemoryLayout.structLayout;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.invoke.MethodHandles.insertCoordinates;

/**
 * Postgresql C library PQconninfoOption definition as memory layout.
 *
 * @author Alireza Pourtaghi
 */
public final class PQConnInfoOption {

    public static final StructLayout PQConnInfoOptionLayout = structLayout(
            ADDRESS.withName("keyword"),
            ADDRESS.withName("envvar"),
            ADDRESS.withName("compiled"),
            ADDRESS.withName("val"),
            ADDRESS.withName("label"),
            ADDRESS.withName("dispchar"),
            JAVA_INT.withName("dispsize"),
            paddingLayout(4)
    ).withName("PQconninfoOption");

    public static final VarHandle PQConnInfoOptionSequence_keyword_varHandle =
            insertCoordinates(PQConnInfoOptionLayout.arrayElementVarHandle(groupElement("keyword")), 1, 0L);

    public static final VarHandle PQConnInfoOptionSequence_val_varHandle =
            insertCoordinates(PQConnInfoOptionLayout.arrayElementVarHandle(groupElement("val")), 1, 0L);
}
