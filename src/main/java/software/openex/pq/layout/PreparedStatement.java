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

package software.openex.pq.layout;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.MemoryLayout.structLayout;
import static java.lang.foreign.MemorySegment.NULL;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.invoke.MethodHandles.insertCoordinates;
import static software.openex.pq.type.FieldFormat.BINARY;
import static software.openex.pq.type.FieldFormat.TEXT;

/**
 * Prepared statement definition as memory layout.
 *
 * @author Alireza Pourtaghi
 */
public final class PreparedStatement extends ClosableLayout {

    public PreparedStatement() {
        this(Arena.ofShared());
    }

    public PreparedStatement(
            final Arena arena) {

        super(arena);
    }

    private static final StructLayout layout = structLayout(
            ADDRESS.withName("stmtName"),
            ADDRESS.withName("query"),
            JAVA_INT.withName("nParams"),
            paddingLayout(4),
            ADDRESS.withName("paramValues"),
            ADDRESS.withName("paramLengths"),
            ADDRESS.withName("paramFormats")
    ).withName("PreparedStatement");

    private static final VarHandle stmtNameElement =
            insertCoordinates(layout.varHandle(groupElement("stmtName")), 1, 0L);

    private static final VarHandle queryElement =
            insertCoordinates(layout.varHandle(groupElement("query")), 1, 0L);

    private static final VarHandle nParamsElement =
            insertCoordinates(layout.varHandle(groupElement("nParams")), 1, 0L);

    private static final VarHandle paramValuesElement =
            insertCoordinates(layout.varHandle(groupElement("paramValues")), 1, 0L);

    private static final VarHandle paramLengthsElement =
            insertCoordinates(layout.varHandle(groupElement("paramLengths")), 1, 0L);

    private static final VarHandle paramFormatsElement =
            insertCoordinates(layout.varHandle(groupElement("paramFormats")), 1, 0L);

    public void setStmtName(
            final String stmtName) {

        stmtNameElement.set(getSegment(), getArena().allocateFrom(stmtName));
    }

    public void setQuery(
            final String query) {

        queryElement.set(getSegment(), getArena().allocateFrom(query));
    }

    public void setNParams(
            final int nParams) {

        nParamsElement.set(getSegment(), nParams);
    }

    /**
     * Adds a text data value to the current not filled parameter in the query specified as $n ($1, $2, etc.).
     * For example if you call this method in a prepared statement for the first time, value is inserted at position $1,
     * subsequent calls will fill $2, $3 and so on.
     *
     * @param value text data value
     */
    public void addTextValue(
            final String value) {

        final var previousNParams = (int) nParamsElement.getAndAdd(getSegment(), 1);
        if (previousNParams == 0) {
            // First parameter to be added.
            final var paramValues = getArena().allocate(ADDRESS, 1);
            paramValues.setAtIndex(ADDRESS, 0, value == null ? NULL : getArena().allocateFrom(value));
            paramValuesElement.set(getSegment(), paramValues);

            final var paramLengths = getArena().allocate(JAVA_INT, 1);
            paramLengths.setAtIndex(JAVA_INT, 0, 0);
            paramLengthsElement.set(getSegment(), paramLengths);

            final var paramFormats = getArena().allocate(JAVA_INT, 1);
            paramFormats.setAtIndex(JAVA_INT, 0, TEXT.getSpecifier());
            paramFormatsElement.set(getSegment(), paramFormats);
        } else if (previousNParams >= 1) {
            // previousNParams >= 1
            final var previousParamValues = (MemorySegment) paramValuesElement.get(getSegment());
            final var newParamValues = getArena().allocate(ADDRESS, previousNParams + 1);
            newParamValues.copyFrom(previousParamValues.reinterpret(ADDRESS.byteSize() * previousNParams));
            newParamValues.setAtIndex(ADDRESS, previousNParams, value == null ? NULL : getArena().allocateFrom(value));
            paramValuesElement.set(getSegment(), newParamValues);

            final var previousParamLengths = (MemorySegment) paramLengthsElement.get(getSegment());
            final var newParamLengths = getArena().allocate(JAVA_INT, previousNParams + 1);
            newParamLengths.copyFrom(previousParamLengths.reinterpret(JAVA_INT.byteSize() * previousNParams));
            newParamLengths.setAtIndex(JAVA_INT, previousNParams, 0);
            paramLengthsElement.set(getSegment(), newParamLengths);

            final var previousParamFormats = (MemorySegment) paramFormatsElement.get(getSegment());
            final var newParamFormats = getArena().allocate(JAVA_INT, previousNParams + 1);
            newParamFormats.copyFrom(previousParamFormats.reinterpret(JAVA_INT.byteSize() * previousNParams));
            newParamFormats.setAtIndex(JAVA_INT, previousNParams, TEXT.getSpecifier());
            paramFormatsElement.set(getSegment(), newParamFormats);
        } else {
            throw new RuntimeException("provided pointer to prepared statement is tampered!");
        }
    }

    /**
     * Adds a binary data value to the current not filled parameter in the query specified as $n ($1, $2, etc.).
     * For example if you call this method in a prepared statement for the first time, value is inserted at position $1,
     * subsequent calls will fill $2, $3 and so on.
     *
     * @param value binary data value as {@link MemorySegment}
     */
    public void addBinaryValue(
            final MemorySegment value) {

        final var previousNParams = (int) nParamsElement.getAndAdd(getSegment(), 1);
        if (previousNParams == 0) {
            // First parameter to be added.
            final var paramValues = getArena().allocate(ADDRESS, 1);
            paramValues.setAtIndex(ADDRESS, 0, value);
            paramValuesElement.set(getSegment(), paramValues);

            final var paramLengths = getArena().allocate(JAVA_INT, 1);
            paramLengths.setAtIndex(JAVA_INT, 0, (int) value.byteSize());
            paramLengthsElement.set(getSegment(), paramLengths);

            final var paramFormats = getArena().allocate(JAVA_INT, 1);
            paramFormats.setAtIndex(JAVA_INT, 0, BINARY.getSpecifier());
            paramFormatsElement.set(getSegment(), paramFormats);
        } else if (previousNParams >= 1) {
            // previousNParams >= 1
            final var previousParamValues = (MemorySegment) paramValuesElement.get(getSegment());
            final var newParamValues = getArena().allocate(ADDRESS, previousNParams + 1);
            newParamValues.copyFrom(previousParamValues.reinterpret(ADDRESS.byteSize() * previousNParams));
            newParamValues.setAtIndex(ADDRESS, previousNParams, value);
            paramValuesElement.set(getSegment(), newParamValues);

            final var previousParamLengths = (MemorySegment) paramLengthsElement.get(getSegment());
            final var newParamLengths = getArena().allocate(JAVA_INT, previousNParams + 1);
            newParamLengths.copyFrom(previousParamLengths.reinterpret(JAVA_INT.byteSize() * previousNParams));
            newParamLengths.setAtIndex(JAVA_INT, previousNParams, (int) value.byteSize());
            paramLengthsElement.set(getSegment(), newParamLengths);

            final var previousParamFormats = (MemorySegment) paramFormatsElement.get(getSegment());
            final var newParamFormats = getArena().allocate(JAVA_INT, previousNParams + 1);
            newParamFormats.copyFrom(previousParamFormats.reinterpret(JAVA_INT.byteSize() * previousNParams));
            newParamFormats.setAtIndex(JAVA_INT, previousNParams, BINARY.getSpecifier());
            paramFormatsElement.set(getSegment(), newParamFormats);
        } else {
            throw new RuntimeException("provided pointer to prepared statement is tampered!");
        }
    }

    @Override
    public MemoryLayout layout() {
        return layout;
    }

    @Override
    public VarHandle var(
            final String name) {

        return switch (name) {
            case "stmtName" -> stmtNameElement;
            case "query" -> queryElement;
            case "nParams" -> nParamsElement;
            case "paramValues" -> paramValuesElement;
            case "paramLengths" -> paramLengthsElement;
            case "paramFormats" -> paramFormatsElement;

            default -> throw new IllegalAccessError();
        };
    }

    @Override
    public VarHandle arrayElementVar(
            final String name) {

        throw new IllegalAccessError();
    }
}
