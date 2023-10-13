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

package ir.jibit.pq;

import ir.jibit.pq.enums.PGPing;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Optional;

import static ir.jibit.pq.layouts.PQConnInfoOption.*;
import static ir.jibit.pq.layouts.PreparedStatement.*;
import static ir.jibit.pq.std.CString.strlen;
import static java.lang.foreign.MemorySegment.NULL;

/**
 * {@link PQ} extended to have some utility methods.
 *
 * @author Alireza Pourtaghi
 */
public final class PQX extends PQ {

    public PQX(
            final Path path) {

        super(path);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">See official doc for more information.</a>
     */
    public Optional<MemorySegment> connectDB(
            final String connInfo) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            final var pgConn = connectDB(arena.allocateUtf8String(connInfo));
            if (!pgConn.equals(NULL)) {
                return Optional.of(pgConn);
            }

            return Optional.empty();
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNINFO">See official doc for more information.</a>
     */
    public Optional<MemorySegment> connInfoOptional(
            final MemorySegment conn) throws Throwable {

        final var connInfo = connInfo(conn);
        if (!connInfo.equals(NULL)) {
            return Optional.of(connInfo);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQPING">See official doc for more information.</a>
     */
    public PGPing ping(
            final String connInfo) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            return ping(arena.allocateUtf8String(connInfo));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">See official doc for more information.</a>
     */
    public Optional<Integer> socketOptional(
            final MemorySegment conn) throws Throwable {

        final var socket = socket(conn);
        if (socket > 0) {
            return Optional.of(socket);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">See official doc for more information.</a>
     */
    public MemorySegment exec(
            final MemorySegment conn,
            final String command) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            return exec(conn, arena.allocateUtf8String(command));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQPREPARE">See official doc for more information.</a>
     */
    public MemorySegment prepare(
            final MemorySegment conn,
            final MemorySegment preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        final var query = (MemorySegment) PreparedStatement_query_varHandle.get(preparedStatement);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatement);

        return prepare(conn, stmtName, query, nParams);
    }

    /**
     * Executes a prepared statement by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement pointer to an instance of {@link ir.jibit.pq.layouts.PreparedStatement} struct
     * @return pointer to a postgresql result model in text format
     */
    public MemorySegment execPreparedTextResult(
            final MemorySegment conn,
            final MemorySegment preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatement);
        final var paramValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatement);
        final var paramLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatement);
        final var paramFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatement);

        return execPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, 0);
    }

    /**
     * Executes a prepared statement by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement pointer to an instance of {@link ir.jibit.pq.layouts.PreparedStatement} struct
     * @return pointer to a postgresql result model in binary format
     */
    public MemorySegment execPreparedBinaryResult(
            final MemorySegment conn,
            final MemorySegment preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatement);
        final var paramValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatement);
        final var paramLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatement);
        final var paramFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatement);

        return execPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, 1);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE">See official doc for more information.</a>
     */
    public String resultErrorMessageString(
            final MemorySegment res) throws Throwable {

        final var resultErrorMessage = resultErrorMessage(res);
        return resultErrorMessage.reinterpret(strlen(resultErrorMessage) + 1).getUtf8String(0);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNAME">See official doc for more information.</a>
     */
    public Optional<String> fNameOptionalString(
            final MemorySegment res,
            final int columnNumber) throws Throwable {

        final var name = fName(res, columnNumber);
        if (!name.equals(NULL)) {
            return Optional.of(name.reinterpret(strlen(name) + 1).getUtf8String(0));
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNUMBER">See official doc for more information.</a>
     */
    public Optional<Integer> fNumberOptional(
            final MemorySegment res,
            final String columnName) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            final var number = fNumber(res, arena.allocateUtf8String(columnName));
            if (number != -1) {
                return Optional.of(number);
            }

            return Optional.empty();
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCMDTUPLES">See official doc for more information.</a>
     */
    public int cmdTuplesInt(
            final MemorySegment res) throws Throwable {

        final var cmdTuples = cmdTuples(res);
        final var countString = cmdTuples.reinterpret(strlen(cmdTuples) + 1).getUtf8String(0);
        if (!countString.isBlank()) {
            return Integer.parseInt(countString);
        }

        return -1;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDPREPARE">See official doc for more information.</a>
     */
    public boolean sendPrepare(
            final MemorySegment conn,
            final MemorySegment preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        final var query = (MemorySegment) PreparedStatement_query_varHandle.get(preparedStatement);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatement);

        return sendPrepare(conn, stmtName, query, nParams);
    }

    /**
     * Executes a prepared statement asynchronously by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement pointer to an instance of {@link ir.jibit.pq.layouts.PreparedStatement} struct
     * @return true if submit was successful otherwise false
     */
    public boolean sendQueryPreparedTextResult(
            final MemorySegment conn,
            final MemorySegment preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatement);
        final var paramValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatement);
        final var paramLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatement);
        final var paramFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatement);

        return sendQueryPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, 0);
    }

    /**
     * Executes a prepared statement asynchronously by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement pointer to an instance of {@link ir.jibit.pq.layouts.PreparedStatement} struct
     * @return true if submit was successful otherwise false
     */
    public boolean sendQueryPreparedBinaryResult(
            final MemorySegment conn,
            final MemorySegment preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatement);
        final var paramValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatement);
        final var paramLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatement);
        final var paramFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatement);

        return sendQueryPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, 1);
    }

    /**
     * Gets the value of a provided connection option keyword.
     *
     * @param conn    memory segment instance returned by connecting to postgresql server
     * @param keyword keyword to search
     * @return optional value associated to provided keyword
     * @throws Throwable in case of errors
     */
    public Optional<String> getConnectionOptionValue(
            final MemorySegment conn,
            final String keyword) throws Throwable {

        final var ptr = connInfoOptional(conn).orElseThrow();
        try {
            for (int i = 0; ; i++) {
                final var rPtr = ptr.reinterpret(PQConnInfoOption.byteSize() + PQConnInfoOption.byteSize() * i);
                final var keywordPtr = (MemorySegment) PQConnInfoOptionSequence_keyword_varHandle.get(rPtr, i);

                if (!keywordPtr.equals(NULL)) {
                    if (keywordPtr.reinterpret(strlen(keywordPtr) + 1).getUtf8String(0).equals(keyword)) {
                        final var valPtr = (MemorySegment) PQConnInfoOptionSequence_val_varHandle.get(rPtr, i);
                        if (!valPtr.equals(NULL)) {
                            // Found keyword and has value.
                            return Optional.of(valPtr.reinterpret(strlen(valPtr) + 1).getUtf8String(0));
                        } else {
                            // Found keyword but its value is null.
                            return Optional.empty();
                        }
                    }
                } else {
                    // We are at the end of the array and could not find keyword!
                    return Optional.empty();
                }
            }
        } finally {
            connInfoFree(ptr);
        }
    }

    /**
     * Prints information about a connection to postgresql server.
     *
     * @param conn memory segment instance returned by connecting to postgresql server
     * @throws Throwable in case of errors
     */
    public void printConnInfo(
            final MemorySegment conn) throws Throwable {

        final var ptr = connInfoOptional(conn).orElseThrow();
        try {
            for (int i = 0; ; i++) {
                final var rPtr = ptr.reinterpret(PQConnInfoOption.byteSize() + PQConnInfoOption.byteSize() * i);
                final var keywordPtr = (MemorySegment) PQConnInfoOptionSequence_keyword_varHandle.get(rPtr, i);
                final var valPtr = (MemorySegment) PQConnInfoOptionSequence_val_varHandle.get(rPtr, i);

                if (keywordPtr.equals(NULL)) {
                    break;
                } else {
                    System.out.print(keywordPtr.reinterpret(strlen(keywordPtr) + 1).getUtf8String(0) + ": ");
                    if (!valPtr.equals(NULL)) {
                        System.out.print(valPtr.reinterpret(strlen(valPtr) + 1).getUtf8String(0));
                    }

                    System.out.println();
                }
            }
        } finally {
            connInfoFree(ptr);
        }
    }
}
