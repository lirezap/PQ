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

package software.openex.pq;

import software.openex.pq.layout.PQConnInfoOption;
import software.openex.pq.layout.PreparedStatement;
import software.openex.pq.type.PGPing;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.lang.foreign.MemorySegment.NULL;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static software.openex.pq.std.CString.strlen;
import static software.openex.pq.type.FieldFormat.BINARY;
import static software.openex.pq.type.FieldFormat.TEXT;

/**
 * {@link PQ} extended to have some utility methods.
 *
 * @author Alireza Pourtaghi
 */
public final class PQX extends PQ {

    /**
     * Same as {@link PQ} constructor.
     *
     * @param path shared object (or dynamic) postgresql C library's path
     */
    public PQX(
            final Path path) {

        super(path);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public Optional<MemorySegment> connectDB(
            final String connInfo) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            final var conn = connectDB(arena.allocateFrom(connInfo));
            if (!conn.equals(NULL)) {
                return of(conn);
            }

            return empty();
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNINFO">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public Optional<MemorySegment> connInfoOptional(
            final MemorySegment conn) throws Throwable {

        final var connInfo = connInfo(conn);
        if (!connInfo.equals(NULL)) {
            return of(connInfo);
        }

        return empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQPING">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public PGPing ping(
            final String connInfo) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            return ping(arena.allocateFrom(connInfo));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQDB">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public String dbString(
            final MemorySegment conn) throws Throwable {

        final var db = db(conn);
        return db.reinterpret(strlen(db) + 1).getString(0);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQERRORMESSAGE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public String errorMessageString(
            final MemorySegment conn) throws Throwable {

        final var errorMessage = errorMessage(conn);
        return errorMessage.reinterpret(strlen(errorMessage) + 1).getString(0);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public Optional<Integer> socketOptional(
            final MemorySegment conn) throws Throwable {

        final var socket = socket(conn);
        if (socket > 0) {
            return of(socket);
        }

        return empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public MemorySegment exec(
            final MemorySegment conn,
            final String command) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            return exec(conn, arena.allocateFrom(command));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQPREPARE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public MemorySegment prepare(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var query = (MemorySegment) preparedStatement.var("query").get(preparedStatement.getSegment());
        final var nParams = (int) preparedStatement.var("nParams").get(preparedStatement.getSegment());

        return prepare(conn, stmtName, query, nParams);
    }

    /**
     * Executes a prepared statement by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement an instance of {@link PreparedStatement}
     * @return pointer to a postgresql result model in text format
     * @throws Throwable in case of any error while calling native function
     */
    public MemorySegment execPreparedTextResult(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var nParams = (int) preparedStatement.var("nParams").get(preparedStatement.getSegment());
        final var paramValues = (MemorySegment) preparedStatement.var("paramValues").get(preparedStatement.getSegment());
        final var paramLengths = (MemorySegment) preparedStatement.var("paramLengths").get(preparedStatement.getSegment());
        final var paramFormats = (MemorySegment) preparedStatement.var("paramFormats").get(preparedStatement.getSegment());

        return execPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, TEXT.getSpecifier());
    }

    /**
     * Executes a prepared statement by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement an instance of {@link PreparedStatement}
     * @return pointer to a postgresql result model in binary format
     * @throws Throwable in case of any error while calling native function
     */
    public MemorySegment execPreparedBinaryResult(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var nParams = (int) preparedStatement.var("nParams").get(preparedStatement.getSegment());
        final var paramValues = (MemorySegment) preparedStatement.var("paramValues").get(preparedStatement.getSegment());
        final var paramLengths = (MemorySegment) preparedStatement.var("paramLengths").get(preparedStatement.getSegment());
        final var paramFormats = (MemorySegment) preparedStatement.var("paramFormats").get(preparedStatement.getSegment());

        return execPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, BINARY.getSpecifier());
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public String resultErrorMessageString(
            final MemorySegment res) throws Throwable {

        final var resultErrorMessage = resultErrorMessage(res);
        return resultErrorMessage.reinterpret(strlen(resultErrorMessage) + 1).getString(0);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNAME">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public Optional<String> fNameOptional(
            final MemorySegment res,
            final int columnNumber) throws Throwable {

        final var name = fName(res, columnNumber);
        if (!name.equals(NULL)) {
            return of(name.reinterpret(strlen(name) + 1).getString(0));
        }

        return empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNUMBER">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public Optional<Integer> fNumberOptional(
            final MemorySegment res,
            final String columnName) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            final var number = fNumber(res, arena.allocateFrom(columnName));
            if (number != -1) {
                return of(number);
            }

            return empty();
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCMDTUPLES">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public int cmdTuplesInt(
            final MemorySegment res) throws Throwable {

        final var cmdTuples = cmdTuples(res);
        final var countString = cmdTuples.reinterpret(strlen(cmdTuples) + 1).getString(0);
        if (!countString.isBlank()) {
            return parseInt(countString);
        }

        return -1;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDPREPARE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public boolean sendPrepare(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var query = (MemorySegment) preparedStatement.var("query").get(preparedStatement.getSegment());
        final var nParams = (int) preparedStatement.var("nParams").get(preparedStatement.getSegment());

        return sendPrepare(conn, stmtName, query, nParams);
    }

    /**
     * Executes a prepared statement asynchronously by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement an instance of {@link PreparedStatement}
     * @return true if submit was successful otherwise false
     * @throws Throwable in case of any error while calling native function
     */
    public boolean sendQueryPreparedTextResult(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement);
        final var nParams = (int) preparedStatement.var("nParams").get(preparedStatement);
        final var paramValues = (MemorySegment) preparedStatement.var("paramValues").get(preparedStatement.getSegment());
        final var paramLengths = (MemorySegment) preparedStatement.var("paramLengths").get(preparedStatement.getSegment());
        final var paramFormats = (MemorySegment) preparedStatement.var("paramFormats").get(preparedStatement.getSegment());

        return sendQueryPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, TEXT.getSpecifier());
    }

    /**
     * Executes a prepared statement asynchronously by using pointers provided as struct fields in parameter.
     *
     * @param conn              postgresql database connection
     * @param preparedStatement an instance of {@link PreparedStatement}
     * @return true if submit was successful otherwise false
     * @throws Throwable in case of any error while calling native function
     */
    public boolean sendQueryPreparedBinaryResult(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var nParams = (int) preparedStatement.var("nParams").get(preparedStatement.getSegment());
        final var paramValues = (MemorySegment) preparedStatement.var("paramValues").get(preparedStatement.getSegment());
        final var paramLengths = (MemorySegment) preparedStatement.var("paramLengths").get(preparedStatement.getSegment());
        final var paramFormats = (MemorySegment) preparedStatement.var("paramFormats").get(preparedStatement.getSegment());

        return sendQueryPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, BINARY.getSpecifier());
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-cancel.html#LIBPQ-PQCANCEL">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public void cancel(
            final MemorySegment cancelPtr) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            final var errBuf = arena.allocate(256);
            final var result = cancel(cancelPtr, errBuf, 256);
            if (result == 0) {
                throw new RuntimeException(errBuf.reinterpret(256).getString(0));
            }
        }
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
                final var rPtr = ptr.reinterpret(
                        PQConnInfoOption.instance().layout().byteSize() +
                                PQConnInfoOption.instance().layout().byteSize() * i);

                final var keywordPtr = (MemorySegment) PQConnInfoOption.instance().arrayElementVar("keyword").get(rPtr, i);
                if (!keywordPtr.equals(NULL)) {
                    if (keywordPtr.reinterpret(strlen(keywordPtr) + 1).getString(0).equals(keyword)) {
                        final var valPtr = (MemorySegment) PQConnInfoOption.instance().arrayElementVar("val").get(rPtr, i);
                        if (!valPtr.equals(NULL)) {
                            // Found keyword and has value.
                            return of(valPtr.reinterpret(strlen(valPtr) + 1).getString(0));
                        } else {
                            // Found keyword but its value is null.
                            return empty();
                        }
                    }
                } else {
                    // We are at the end of the array and could not find keyword!
                    return empty();
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
                final var rPtr = ptr.reinterpret(
                        PQConnInfoOption.instance().layout().byteSize() +
                                PQConnInfoOption.instance().layout().byteSize() * i);

                final var keywordPtr = (MemorySegment) PQConnInfoOption.instance().arrayElementVar("keyword").get(rPtr, i);
                final var valPtr = (MemorySegment) PQConnInfoOption.instance().arrayElementVar("val").get(rPtr, i);

                if (keywordPtr.equals(NULL)) {
                    break;
                } else {
                    System.out.print(keywordPtr.reinterpret(strlen(keywordPtr) + 1).getString(0) + ": ");
                    if (!valPtr.equals(NULL)) {
                        System.out.print(valPtr.reinterpret(strlen(valPtr) + 1).getString(0));
                    }

                    System.out.println();
                }
            }
        } finally {
            connInfoFree(ptr);
        }
    }
}
