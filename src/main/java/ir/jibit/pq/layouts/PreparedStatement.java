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

package ir.jibit.pq.layouts;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.MemoryLayout.structLayout;
import static java.lang.foreign.MemorySegment.NULL;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.util.Objects.requireNonNull;

/**
 * Prepared statement definition as memory layout.
 *
 * @author Alireza Pourtaghi
 */
public final class PreparedStatement {

    public static final StructLayout PreparedStatement = structLayout(
            ADDRESS.withName("stmtName"),
            ADDRESS.withName("query"),
            JAVA_INT.withName("nParams"),
            paddingLayout(4),
            ADDRESS.withName("paramValues"),
            ADDRESS.withName("paramLengths"),
            ADDRESS.withName("paramFormats")
    );

    public static final VarHandle PreparedStatement_stmtName_varHandle =
            PreparedStatement.varHandle(groupElement("stmtName"));

    public static final VarHandle PreparedStatement_query_varHandle =
            PreparedStatement.varHandle(groupElement("query"));

    public static final VarHandle PreparedStatement_nParams_varHandle =
            PreparedStatement.varHandle(groupElement("nParams"));

    public static final VarHandle PreparedStatement_paramValues_varHandle =
            PreparedStatement.varHandle(groupElement("paramValues"));

    public static final VarHandle PreparedStatement_paramLengths_varHandle =
            PreparedStatement.varHandle(groupElement("paramLengths"));

    public static final VarHandle PreparedStatement_paramFormats_varHandle =
            PreparedStatement.varHandle(groupElement("paramFormats"));

    /**
     * Creates a new prepared statement in provided memory arena with all default values for fields.
     *
     * @param arena memory arena
     * @return a new created memory segment for a new prepared statement
     */
    public static MemorySegment create(final Arena arena) {
        requireNonNull(arena);
        return arena.allocate(PreparedStatement);
    }

    public static void setStmtName(final Arena arena, final MemorySegment preparedStatement, final String stmtName) {
        requireNonNull(arena);
        requireNonNull(preparedStatement);
        requireNonNull(stmtName);
        PreparedStatement_stmtName_varHandle.set(preparedStatement, arena.allocateUtf8String(stmtName));
    }

    public static void setQuery(final Arena arena, final MemorySegment preparedStatement, final String query) {
        requireNonNull(arena);
        requireNonNull(preparedStatement);
        requireNonNull(query);
        PreparedStatement_query_varHandle.set(preparedStatement, arena.allocateUtf8String(query));
    }

    public static void addTextValue(final Arena arena, final MemorySegment preparedStatement, final String value) {
        requireNonNull(arena);
        requireNonNull(preparedStatement);
        final var previousNParams = (int) PreparedStatement_nParams_varHandle.getAndAdd(preparedStatement, 1);

        if (previousNParams == 0) {
            // First parameter to be added.
            final var paramValues = arena.allocateArray(ADDRESS, 1);
            paramValues.setAtIndex(ADDRESS, 0, value == null ? NULL : arena.allocateUtf8String(value));
            PreparedStatement_paramValues_varHandle.set(preparedStatement, paramValues);

            final var paramLengths = arena.allocateArray(JAVA_INT, 1);
            paramLengths.setAtIndex(JAVA_INT, 0, 0);
            PreparedStatement_paramLengths_varHandle.set(preparedStatement, paramLengths);

            final var paramFormats = arena.allocateArray(JAVA_INT, 1);
            paramFormats.setAtIndex(JAVA_INT, 0, 0);
            PreparedStatement_paramFormats_varHandle.set(preparedStatement, paramFormats);
        } else if (previousNParams >= 1) {
            // previousNParams >= 1
            final var previousParamValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatement);
            final var newParamValues = arena.allocateArray(ADDRESS, previousNParams + 1);
            newParamValues.copyFrom(previousParamValues.reinterpret(ADDRESS.byteSize() * (previousNParams + 1)));
            newParamValues.setAtIndex(ADDRESS, previousNParams, value == null ? NULL : arena.allocateUtf8String(value));
            PreparedStatement_paramValues_varHandle.set(preparedStatement, newParamValues);

            final var previousParamLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatement);
            final var newParamLengths = arena.allocateArray(JAVA_INT, previousNParams + 1);
            newParamLengths.copyFrom(previousParamLengths.reinterpret(JAVA_INT.byteSize() * (previousNParams + 1)));
            newParamLengths.setAtIndex(JAVA_INT, previousNParams, 0);
            PreparedStatement_paramLengths_varHandle.set(preparedStatement, newParamLengths);

            final var previousParamFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatement);
            final var newParamFormats = arena.allocateArray(JAVA_INT, previousNParams + 1);
            newParamFormats.copyFrom(previousParamFormats.reinterpret(JAVA_INT.byteSize() * (previousNParams + 1)));
            newParamFormats.setAtIndex(JAVA_INT, previousNParams, 0);
            PreparedStatement_paramFormats_varHandle.set(preparedStatement, newParamFormats);
        } else {
            throw new RuntimeException("provided pointer to prepared statement is tampered!");
        }
    }
}
