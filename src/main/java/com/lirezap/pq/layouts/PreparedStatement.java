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

import com.lirezap.pq.types.FieldFormat;

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

    public static final StructLayout PreparedStatementLayout = structLayout(
            ADDRESS.withName("stmtName"),
            ADDRESS.withName("query"),
            JAVA_INT.withName("nParams"),
            paddingLayout(4),
            ADDRESS.withName("paramValues"),
            ADDRESS.withName("paramLengths"),
            ADDRESS.withName("paramFormats")
    );

    public static final VarHandle PreparedStatement_stmtName_varHandle =
            PreparedStatementLayout.varHandle(groupElement("stmtName"));

    public static final VarHandle PreparedStatement_query_varHandle =
            PreparedStatementLayout.varHandle(groupElement("query"));

    public static final VarHandle PreparedStatement_nParams_varHandle =
            PreparedStatementLayout.varHandle(groupElement("nParams"));

    public static final VarHandle PreparedStatement_paramValues_varHandle =
            PreparedStatementLayout.varHandle(groupElement("paramValues"));

    public static final VarHandle PreparedStatement_paramLengths_varHandle =
            PreparedStatementLayout.varHandle(groupElement("paramLengths"));

    public static final VarHandle PreparedStatement_paramFormats_varHandle =
            PreparedStatementLayout.varHandle(groupElement("paramFormats"));

    /**
     * Creates a new prepared statement in provided memory arena with all default values for fields.
     *
     * @param arena memory arena
     * @return a new created memory segment for a new prepared statement
     */
    public static MemorySegment create(
            final Arena arena) {

        requireNonNull(arena);
        return arena.allocate(PreparedStatementLayout);
    }

    public static void setStmtName(
            final Arena arena,
            final MemorySegment preparedStatement,
            final String stmtName) {

        requireNonNull(arena);
        requireNonNull(preparedStatement);
        requireNonNull(stmtName);
        PreparedStatement_stmtName_varHandle.set(preparedStatement, arena.allocateUtf8String(stmtName));
    }

    public static void setQuery(
            final Arena arena,
            final MemorySegment preparedStatement,
            final String query) {

        requireNonNull(arena);
        requireNonNull(preparedStatement);
        requireNonNull(query);
        PreparedStatement_query_varHandle.set(preparedStatement, arena.allocateUtf8String(query));
    }

    public static void setNParams(
            final MemorySegment preparedStatement,
            final int nParams) {

        requireNonNull(preparedStatement);
        PreparedStatement_nParams_varHandle.set(preparedStatement, nParams);
    }

    /**
     * Adds a text data value to the current not filled parameter in the query specified as $n ($1, $2, etc.).
     * For example if you call this method in a prepared statement for the first time, value is inserted at position $1,
     * subsequent calls will fill $2, $3 and so on.
     *
     * @param arena             the arena that prepared statement created at
     * @param preparedStatement prepared statement that must be used to add a text data value into
     * @param value             text data value
     */
    public static void addTextValue(
            final Arena arena,
            final MemorySegment preparedStatement,
            final String value) {

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
            paramFormats.setAtIndex(JAVA_INT, 0, FieldFormat.TEXT.getSpecifier());
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
            newParamFormats.setAtIndex(JAVA_INT, previousNParams, FieldFormat.TEXT.getSpecifier());
            PreparedStatement_paramFormats_varHandle.set(preparedStatement, newParamFormats);
        } else {
            throw new RuntimeException("provided pointer to prepared statement is tampered!");
        }
    }

    /**
     * Adds a binary data value to the current not filled parameter in the query specified as $n ($1, $2, etc.).
     * For example if you call this method in a prepared statement for the first time, value is inserted at position $1,
     * subsequent calls will fill $2, $3 and so on.
     *
     * @param arena             the arena that prepared statement created at
     * @param preparedStatement prepared statement that must be used to add a binary data value into
     * @param value             binary data value as memory segment
     */
    public static void addBinaryValue(
            final Arena arena,
            final MemorySegment preparedStatement,
            final MemorySegment value) {

        requireNonNull(arena);
        requireNonNull(preparedStatement);
        requireNonNull(value);

        if (value.byteSize() < 0 || value.byteSize() > Integer.MAX_VALUE) {
            throw new RuntimeException("memory segment's size is not valid");
        }

        final var previousNParams = (int) PreparedStatement_nParams_varHandle.getAndAdd(preparedStatement, 1);
        if (previousNParams == 0) {
            // First parameter to be added.
            final var paramValues = arena.allocateArray(ADDRESS, 1);
            paramValues.setAtIndex(ADDRESS, 0, value);
            PreparedStatement_paramValues_varHandle.set(preparedStatement, paramValues);

            final var paramLengths = arena.allocateArray(JAVA_INT, 1);
            paramLengths.setAtIndex(JAVA_INT, 0, (int) value.byteSize());
            PreparedStatement_paramLengths_varHandle.set(preparedStatement, paramLengths);

            final var paramFormats = arena.allocateArray(JAVA_INT, 1);
            paramFormats.setAtIndex(JAVA_INT, 0, FieldFormat.BINARY.getSpecifier());
            PreparedStatement_paramFormats_varHandle.set(preparedStatement, paramFormats);
        } else if (previousNParams >= 1) {
            // previousNParams >= 1
            final var previousParamValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatement);
            final var newParamValues = arena.allocateArray(ADDRESS, previousNParams + 1);
            newParamValues.copyFrom(previousParamValues.reinterpret(ADDRESS.byteSize() * (previousNParams + 1)));
            newParamValues.setAtIndex(ADDRESS, previousNParams, value.equals(NULL) ? NULL : value);
            PreparedStatement_paramValues_varHandle.set(preparedStatement, newParamValues);

            final var previousParamLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatement);
            final var newParamLengths = arena.allocateArray(JAVA_INT, previousNParams + 1);
            newParamLengths.copyFrom(previousParamLengths.reinterpret(JAVA_INT.byteSize() * (previousNParams + 1)));
            newParamLengths.setAtIndex(JAVA_INT, previousNParams, (int) value.byteSize());
            PreparedStatement_paramLengths_varHandle.set(preparedStatement, newParamLengths);

            final var previousParamFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatement);
            final var newParamFormats = arena.allocateArray(JAVA_INT, previousNParams + 1);
            newParamFormats.copyFrom(previousParamFormats.reinterpret(JAVA_INT.byteSize() * (previousNParams + 1)));
            newParamFormats.setAtIndex(JAVA_INT, previousNParams, FieldFormat.BINARY.getSpecifier());
            PreparedStatement_paramFormats_varHandle.set(preparedStatement, newParamFormats);
        } else {
            throw new RuntimeException("provided pointer to prepared statement is tampered!");
        }
    }
}
