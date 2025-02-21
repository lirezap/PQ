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

package software.openex.pq.std;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.FunctionDescriptor.of;
import static java.lang.foreign.Linker.nativeLinker;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * C string handling functions.
 *
 * @author Alireza Pourtaghi
 */
public final class CString {

    /**
     * Library symbols; including functions and variables.
     */
    private static final SymbolLookup lib = nativeLinker().defaultLookup();

    private static final MethodHandle strlenHandle =
            nativeLinker().downcallHandle(lib.find(FUNCTION.strlen.name()).orElseThrow(), FUNCTION.strlen.fd);

    public static long strlen(
            final MemorySegment string) throws Throwable {

        return (long) strlenHandle.invokeExact(string);
    }

    /**
     * Name and descriptor of C string handling functions.
     *
     * @author Alireza Pourtaghi
     */
    private enum FUNCTION {
        strlen(of(JAVA_LONG, ADDRESS));

        public final FunctionDescriptor fd;

        FUNCTION(
                final FunctionDescriptor fd) {

            this.fd = fd;
        }
    }
}
