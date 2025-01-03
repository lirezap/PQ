/*
 * ISC License
 *
 * Copyright (c) 2025, Alireza Pourtaghi <lirezap@protonmail.com>
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

package com.lirezap.pq;

import com.lirezap.pq.type.*;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.Arena.ofShared;
import static java.lang.foreign.FunctionDescriptor.of;
import static java.lang.foreign.FunctionDescriptor.ofVoid;
import static java.lang.foreign.Linker.nativeLinker;
import static java.lang.foreign.MemorySegment.NULL;
import static java.lang.foreign.SymbolLookup.libraryLookup;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Java FFM used wrapper of postgresql C library functions.
 *
 * @author Alireza Pourtaghi
 */
public sealed class PQ implements AutoCloseable permits PQX {

    /**
     * Memory region that is needed to load library into.
     */
    private final Arena memory;

    // Database Connection Control Functions
    private final MethodHandle connectDBHandle;
    private final MethodHandle connInfoHandle;
    private final MethodHandle finishHandle;
    private final MethodHandle resetHandle;
    private final MethodHandle pingHandle;

    // Connection Status Functions
    private final MethodHandle dbHandle;
    private final MethodHandle statusHandle;
    private final MethodHandle transactionStatusHandle;
    private final MethodHandle protocolVersionHandle;
    private final MethodHandle serverVersionHandle;
    private final MethodHandle errorMessageHandle;
    private final MethodHandle socketHandle;
    private final MethodHandle backendPIDHandle;

    // Command Execution Functions
    private final MethodHandle execHandle;
    private final MethodHandle prepareHandle;
    private final MethodHandle execPreparedHandle;
    private final MethodHandle describePreparedHandle;
    private final MethodHandle resultStatusHandle;
    private final MethodHandle resultErrorMessageHandle;
    private final MethodHandle clearHandle;
    private final MethodHandle nTuplesHandle;
    private final MethodHandle nFieldsHandle;
    private final MethodHandle fNameHandle;
    private final MethodHandle fNumberHandle;
    private final MethodHandle fFormatHandle;
    private final MethodHandle fTypeHandle;
    private final MethodHandle fModHandle;
    private final MethodHandle getValueHandle;
    private final MethodHandle getIsNullHandle;
    private final MethodHandle getLengthHandle;
    private final MethodHandle cmdTuplesHandle;

    // Asynchronous Command Processing
    private final MethodHandle sendQueryHandle;
    private final MethodHandle sendPrepareHandle;
    private final MethodHandle sendQueryPreparedHandle;
    private final MethodHandle sendDescribePreparedHandle;
    private final MethodHandle getResultHandle;
    private final MethodHandle consumeInputHandle;
    private final MethodHandle isBusyHandle;

    // Canceling Queries In Progress
    private final MethodHandle getCancelHandle;
    private final MethodHandle freeCancelHandle;
    private final MethodHandle cancelHandle;

    // Miscellaneous Functions
    private final MethodHandle connInfoFreeHandle;

    /**
     * Creates memory allocator, native linker and library lookup instance to load shared object (or dynamic) postgresql
     * C library from provided path.
     *
     * @param path shared object (or dynamic) postgresql C library's path
     */
    public PQ(
            final Path path) {

        this.memory = ofShared();
        final var linker = nativeLinker();
        final var lib = libraryLookup(path, memory);

        // Database Connection Control Functions
        this.connectDBHandle = linker.downcallHandle(lib.find(FUNCTION.PQconnectdb.name()).orElseThrow(), FUNCTION.PQconnectdb.fd);
        this.connInfoHandle = linker.downcallHandle(lib.find(FUNCTION.PQconninfo.name()).orElseThrow(), FUNCTION.PQconninfo.fd);
        this.finishHandle = linker.downcallHandle(lib.find(FUNCTION.PQfinish.name()).orElseThrow(), FUNCTION.PQfinish.fd);
        this.resetHandle = linker.downcallHandle(lib.find(FUNCTION.PQreset.name()).orElseThrow(), FUNCTION.PQreset.fd);
        this.pingHandle = linker.downcallHandle(lib.find(FUNCTION.PQping.name()).orElseThrow(), FUNCTION.PQping.fd);

        // Connection Status Functions
        this.dbHandle = linker.downcallHandle(lib.find(FUNCTION.PQdb.name()).orElseThrow(), FUNCTION.PQdb.fd);
        this.statusHandle = linker.downcallHandle(lib.find(FUNCTION.PQstatus.name()).orElseThrow(), FUNCTION.PQstatus.fd);
        this.transactionStatusHandle = linker.downcallHandle(lib.find(FUNCTION.PQtransactionStatus.name()).orElseThrow(), FUNCTION.PQtransactionStatus.fd);
        this.protocolVersionHandle = linker.downcallHandle(lib.find(FUNCTION.PQprotocolVersion.name()).orElseThrow(), FUNCTION.PQprotocolVersion.fd);
        this.serverVersionHandle = linker.downcallHandle(lib.find(FUNCTION.PQserverVersion.name()).orElseThrow(), FUNCTION.PQserverVersion.fd);
        this.errorMessageHandle = linker.downcallHandle(lib.find(FUNCTION.PQerrorMessage.name()).orElseThrow(), FUNCTION.PQerrorMessage.fd);
        this.socketHandle = linker.downcallHandle(lib.find(FUNCTION.PQsocket.name()).orElseThrow(), FUNCTION.PQsocket.fd);
        this.backendPIDHandle = linker.downcallHandle(lib.find(FUNCTION.PQbackendPID.name()).orElseThrow(), FUNCTION.PQbackendPID.fd);

        // Command Execution Functions
        this.execHandle = linker.downcallHandle(lib.find(FUNCTION.PQexec.name()).orElseThrow(), FUNCTION.PQexec.fd);
        this.prepareHandle = linker.downcallHandle(lib.find(FUNCTION.PQprepare.name()).orElseThrow(), FUNCTION.PQprepare.fd);
        this.execPreparedHandle = linker.downcallHandle(lib.find(FUNCTION.PQexecPrepared.name()).orElseThrow(), FUNCTION.PQexecPrepared.fd);
        this.describePreparedHandle = linker.downcallHandle(lib.find(FUNCTION.PQdescribePrepared.name()).orElseThrow(), FUNCTION.PQdescribePrepared.fd);
        this.resultStatusHandle = linker.downcallHandle(lib.find(FUNCTION.PQresultStatus.name()).orElseThrow(), FUNCTION.PQresultStatus.fd);
        this.resultErrorMessageHandle = linker.downcallHandle(lib.find(FUNCTION.PQresultErrorMessage.name()).orElseThrow(), FUNCTION.PQresultErrorMessage.fd);
        this.clearHandle = linker.downcallHandle(lib.find(FUNCTION.PQclear.name()).orElseThrow(), FUNCTION.PQclear.fd);
        this.nTuplesHandle = linker.downcallHandle(lib.find(FUNCTION.PQntuples.name()).orElseThrow(), FUNCTION.PQntuples.fd);
        this.nFieldsHandle = linker.downcallHandle(lib.find(FUNCTION.PQnfields.name()).orElseThrow(), FUNCTION.PQnfields.fd);
        this.fNameHandle = linker.downcallHandle(lib.find(FUNCTION.PQfname.name()).orElseThrow(), FUNCTION.PQfname.fd);
        this.fNumberHandle = linker.downcallHandle(lib.find(FUNCTION.PQfnumber.name()).orElseThrow(), FUNCTION.PQfnumber.fd);
        this.fFormatHandle = linker.downcallHandle(lib.find(FUNCTION.PQfformat.name()).orElseThrow(), FUNCTION.PQfformat.fd);
        this.fTypeHandle = linker.downcallHandle(lib.find(FUNCTION.PQftype.name()).orElseThrow(), FUNCTION.PQftype.fd);
        this.fModHandle = linker.downcallHandle(lib.find(FUNCTION.PQfmod.name()).orElseThrow(), FUNCTION.PQfmod.fd);
        this.getValueHandle = linker.downcallHandle(lib.find(FUNCTION.PQgetvalue.name()).orElseThrow(), FUNCTION.PQgetvalue.fd);
        this.getIsNullHandle = linker.downcallHandle(lib.find(FUNCTION.PQgetisnull.name()).orElseThrow(), FUNCTION.PQgetisnull.fd);
        this.getLengthHandle = linker.downcallHandle(lib.find(FUNCTION.PQgetlength.name()).orElseThrow(), FUNCTION.PQgetlength.fd);
        this.cmdTuplesHandle = linker.downcallHandle(lib.find(FUNCTION.PQcmdTuples.name()).orElseThrow(), FUNCTION.PQcmdTuples.fd);

        // Asynchronous Command Processing
        this.sendQueryHandle = linker.downcallHandle(lib.find(FUNCTION.PQsendQuery.name()).orElseThrow(), FUNCTION.PQsendQuery.fd);
        this.sendPrepareHandle = linker.downcallHandle(lib.find(FUNCTION.PQsendPrepare.name()).orElseThrow(), FUNCTION.PQsendPrepare.fd);
        this.sendQueryPreparedHandle = linker.downcallHandle(lib.find(FUNCTION.PQsendQueryPrepared.name()).orElseThrow(), FUNCTION.PQsendQueryPrepared.fd);
        this.sendDescribePreparedHandle = linker.downcallHandle(lib.find(FUNCTION.PQsendDescribePrepared.name()).orElseThrow(), FUNCTION.PQsendDescribePrepared.fd);
        this.getResultHandle = linker.downcallHandle(lib.find(FUNCTION.PQgetResult.name()).orElseThrow(), FUNCTION.PQgetResult.fd);
        this.consumeInputHandle = linker.downcallHandle(lib.find(FUNCTION.PQconsumeInput.name()).orElseThrow(), FUNCTION.PQconsumeInput.fd);
        this.isBusyHandle = linker.downcallHandle(lib.find(FUNCTION.PQisBusy.name()).orElseThrow(), FUNCTION.PQisBusy.fd);

        // Canceling Queries In Progress
        this.getCancelHandle = linker.downcallHandle(lib.find(FUNCTION.PQgetCancel.name()).orElseThrow(), FUNCTION.PQgetCancel.fd);
        this.freeCancelHandle = linker.downcallHandle(lib.find(FUNCTION.PQfreeCancel.name()).orElseThrow(), FUNCTION.PQfreeCancel.fd);
        this.cancelHandle = linker.downcallHandle(lib.find(FUNCTION.PQcancel.name()).orElseThrow(), FUNCTION.PQcancel.fd);

        // Miscellaneous Functions
        this.connInfoFreeHandle = linker.downcallHandle(lib.find(FUNCTION.PQconninfoFree.name()).orElseThrow(), FUNCTION.PQconninfoFree.fd);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment connectDB(
            final MemorySegment connInfo) throws Throwable {

        return (MemorySegment) connectDBHandle.invokeExact(connInfo);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNINFO">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment connInfo(
            final MemorySegment conn) throws Throwable {

        return (MemorySegment) connInfoHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQFINISH">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final void finish(
            final MemorySegment conn) throws Throwable {

        finishHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQRESET">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final void reset(
            final MemorySegment conn) throws Throwable {

        resetHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQPING">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final PGPing ping(
            final MemorySegment connInfo) throws Throwable {

        return switch ((int) pingHandle.invokeExact(connInfo)) {
            case 0 -> PGPing.PQPING_OK;
            case 1 -> PGPing.PQPING_REJECT;
            case 2 -> PGPing.PQPING_NO_RESPONSE;
            case 3 -> PGPing.PQPING_NO_ATTEMPT;
            default -> PGPing.UNKNOWN;
        };
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQDB">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment db(
            final MemorySegment conn) throws Throwable {

        return (MemorySegment) dbHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSTATUS">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final ConnStatusType status(
            final MemorySegment conn) throws Throwable {

        return switch ((int) statusHandle.invokeExact(conn)) {
            case 0 -> ConnStatusType.CONNECTION_OK;
            case 1 -> ConnStatusType.CONNECTION_BAD;
            default -> ConnStatusType.UNKNOWN;
        };
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQTRANSACTIONSTATUS">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final PGTransactionStatusType transactionStatus(
            final MemorySegment conn) throws Throwable {

        return switch ((int) transactionStatusHandle.invokeExact(conn)) {
            case 0 -> PGTransactionStatusType.PQTRANS_IDLE;
            case 1 -> PGTransactionStatusType.PQTRANS_ACTIVE;
            case 2 -> PGTransactionStatusType.PQTRANS_INTRANS;
            case 3 -> PGTransactionStatusType.PQTRANS_INERROR;
            case 4 -> PGTransactionStatusType.PQTRANS_UNKNOWN;
            default -> PGTransactionStatusType.UNKNOWN;
        };
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQPROTOCOLVERSION">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int protocolVersion(
            final MemorySegment conn) throws Throwable {

        return (int) protocolVersionHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSERVERVERSION">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int serverVersion(
            final MemorySegment conn) throws Throwable {

        return (int) serverVersionHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQERRORMESSAGE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment errorMessage(
            final MemorySegment conn) throws Throwable {

        return (MemorySegment) errorMessageHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int socket(
            final MemorySegment conn) throws Throwable {

        return (int) socketHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQBACKENDPID">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int backendPID(
            final MemorySegment conn) throws Throwable {

        return (int) backendPIDHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment exec(
            final MemorySegment conn,
            final MemorySegment command) throws Throwable {

        return (MemorySegment) execHandle.invokeExact(conn, command);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQPREPARE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment prepare(
            final MemorySegment conn,
            final MemorySegment stmtName,
            final MemorySegment query,
            final int nParams) throws Throwable {

        return (MemorySegment) prepareHandle.invokeExact(conn, stmtName, query, nParams, NULL);
    }

    /**
     * <a href="https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQEXECPREPARED">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment execPrepared(
            final MemorySegment conn,
            final MemorySegment stmtName,
            final int nParams,
            final MemorySegment paramValues,
            final MemorySegment paramLengths,
            final MemorySegment paramFormats,
            final int resultFormat) throws Throwable {

        return (MemorySegment) execPreparedHandle.invokeExact(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQDESCRIBEPREPARED">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment describePrepared(
            final MemorySegment conn,
            final MemorySegment stmtName) throws Throwable {

        return (MemorySegment) describePreparedHandle.invokeExact(conn, stmtName);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTSTATUS">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final ExecStatusType resultStatus(
            final MemorySegment res) throws Throwable {

        return switch ((int) resultStatusHandle.invokeExact(res)) {
            case 0 -> ExecStatusType.PGRES_EMPTY_QUERY;
            case 1 -> ExecStatusType.PGRES_COMMAND_OK;
            case 2 -> ExecStatusType.PGRES_TUPLES_OK;
            case 3 -> ExecStatusType.PGRES_COPY_OUT;
            case 4 -> ExecStatusType.PGRES_COPY_IN;
            case 5 -> ExecStatusType.PGRES_BAD_RESPONSE;
            case 6 -> ExecStatusType.PGRES_NONFATAL_ERROR;
            case 7 -> ExecStatusType.PGRES_FATAL_ERROR;
            case 8 -> ExecStatusType.PGRES_COPY_BOTH;
            case 9 -> ExecStatusType.PGRES_SINGLE_TUPLE;
            case 10 -> ExecStatusType.PGRES_PIPELINE_SYNC;
            case 11 -> ExecStatusType.PGRES_PIPELINE_ABORTED;
            default -> ExecStatusType.UNKNOWN;
        };
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment resultErrorMessage(
            final MemorySegment res) throws Throwable {

        return (MemorySegment) resultErrorMessageHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCLEAR">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final void clear(
            final MemorySegment res) throws Throwable {

        clearHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQNTUPLES">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int nTuples(
            final MemorySegment res) throws Throwable {

        return (int) nTuplesHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQNFIELDS">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int nFields(
            final MemorySegment res) throws Throwable {

        return (int) nFieldsHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNAME">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment fName(
            final MemorySegment res,
            final int columnNumber) throws Throwable {

        return (MemorySegment) fNameHandle.invokeExact(res, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNUMBER">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int fNumber(
            final MemorySegment res,
            final MemorySegment columnName) throws Throwable {

        return (int) fNumberHandle.invokeExact(res, columnName);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFFORMAT">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final FieldFormat fFormat(
            final MemorySegment res,
            final int columnNumber) throws Throwable {

        return switch ((int) fFormatHandle.invokeExact(res, columnNumber)) {
            case 0 -> FieldFormat.TEXT;
            case 1 -> FieldFormat.BINARY;
            default -> FieldFormat.UNKNOWN;
        };
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFTYPE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int fType(
            final MemorySegment res,
            final int columnNumber) throws Throwable {

        return (int) fTypeHandle.invokeExact(res, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFMOD">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int fMod(
            final MemorySegment res,
            final int columnNumber) throws Throwable {

        return (int) fModHandle.invokeExact(res, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQGETVALUE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment getValue(
            final MemorySegment res,
            final int rowNumber,
            final int columnNumber) throws Throwable {

        return (MemorySegment) getValueHandle.invokeExact(res, rowNumber, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQGETISNULL">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean getIsNull(
            final MemorySegment res,
            final int rowNumber,
            final int columnNumber) throws Throwable {

        return (int) getIsNullHandle.invokeExact(res, rowNumber, columnNumber) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQGETLENGTH">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int getLength(
            final MemorySegment res,
            final int rowNumber,
            final int columnNumber) throws Throwable {

        return (int) getLengthHandle.invokeExact(res, rowNumber, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCMDTUPLES">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment cmdTuples(
            final MemorySegment res) throws Throwable {

        return (MemorySegment) cmdTuplesHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDQUERY">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean sendQuery(
            final MemorySegment conn,
            final MemorySegment command) throws Throwable {

        return (int) sendQueryHandle.invokeExact(conn, command) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDPREPARE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean sendPrepare(
            final MemorySegment conn,
            final MemorySegment stmtName,
            final MemorySegment query,
            final int nParams) throws Throwable {

        return (int) sendPrepareHandle.invokeExact(conn, stmtName, query, nParams, NULL) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDQUERYPREPARED">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean sendQueryPrepared(
            final MemorySegment conn,
            final MemorySegment stmtName,
            final int nParams,
            final MemorySegment paramValues,
            final MemorySegment paramLengths,
            final MemorySegment paramFormats,
            final int resultFormat) throws Throwable {

        return (int) sendQueryPreparedHandle.invokeExact(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDDESCRIBEPREPARED">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean sendDescribePrepared(
            final MemorySegment conn,
            final MemorySegment stmtName) throws Throwable {

        return (int) sendDescribePreparedHandle.invokeExact(conn, stmtName) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQGETRESULT">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment getResult(
            final MemorySegment conn) throws Throwable {

        return (MemorySegment) getResultHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQCONSUMEINPUT">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean consumeInput(
            final MemorySegment conn) throws Throwable {

        return (int) consumeInputHandle.invokeExact(conn) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQISBUSY">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final boolean isBusy(
            final MemorySegment conn) throws Throwable {

        return (int) isBusyHandle.invokeExact(conn) != 0;
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-cancel.html#LIBPQ-PQGETCANCEL">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final MemorySegment getCancel(
            final MemorySegment conn) throws Throwable {

        return (MemorySegment) getCancelHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-cancel.html#LIBPQ-PQFREECANCEL">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final void freeCancel(
            final MemorySegment cancelPtr) throws Throwable {

        freeCancelHandle.invokeExact(cancelPtr);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-cancel.html#LIBPQ-PQCANCEL">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final int cancel(
            final MemorySegment cancelPtr,
            final MemorySegment errBuf,
            final int errBufSize) throws Throwable {

        return (int) cancelHandle.invokeExact(cancelPtr, errBuf, errBufSize);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-misc.html#LIBPQ-PQCONNINFOFREE">See official doc for more information.</a>
     *
     * @throws Throwable in case of any error while calling native function
     */
    public final void connInfoFree(
            final MemorySegment connOptions) throws Throwable {

        connInfoFreeHandle.invokeExact(connOptions);
    }

    @Override
    public final void close() throws Exception {
        memory.close();
    }

    /**
     * Name and descriptor of loaded C functions.
     * <p>See <a href="https://www.postgresql.org/docs/16/libpq.html">here</a> for more information.</p>
     *
     * @author Alireza Pourtaghi
     */
    private enum FUNCTION {
        // Database Connection Control Functions
        PQconnectdb(of(ADDRESS, ADDRESS)),
        PQconninfo(of(ADDRESS, ADDRESS)),
        PQfinish(ofVoid(ADDRESS)),
        PQreset(ofVoid(ADDRESS)),
        PQping(of(JAVA_INT, ADDRESS)),

        // Connection Status Functions
        PQdb(of(ADDRESS, ADDRESS)),
        PQstatus(of(JAVA_INT, ADDRESS)),
        PQtransactionStatus(of(JAVA_INT, ADDRESS)),
        PQprotocolVersion(of(JAVA_INT, ADDRESS)),
        PQserverVersion(of(JAVA_INT, ADDRESS)),
        PQerrorMessage(of(ADDRESS, ADDRESS)),
        PQsocket(of(JAVA_INT, ADDRESS)),
        PQbackendPID(of(JAVA_INT, ADDRESS)),

        // Command Execution Functions
        PQexec(of(ADDRESS, ADDRESS, ADDRESS)),
        PQprepare(of(ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS)),
        PQexecPrepared(of(ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT)),
        PQdescribePrepared(of(ADDRESS, ADDRESS, ADDRESS)),
        PQresultStatus(of(JAVA_INT, ADDRESS)),
        PQresultErrorMessage(of(ADDRESS, ADDRESS)),
        PQclear(ofVoid(ADDRESS)),
        PQntuples(of(JAVA_INT, ADDRESS)),
        PQnfields(of(JAVA_INT, ADDRESS)),
        PQfname(of(ADDRESS, ADDRESS, JAVA_INT)),
        PQfnumber(of(JAVA_INT, ADDRESS, ADDRESS)),
        PQfformat(of(JAVA_INT, ADDRESS, JAVA_INT)),
        PQftype(of(JAVA_INT, ADDRESS, JAVA_INT)),
        PQfmod(of(JAVA_INT, ADDRESS, JAVA_INT)),
        PQgetvalue(of(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT)),
        PQgetisnull(of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)),
        PQgetlength(of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)),
        PQcmdTuples(of(ADDRESS, ADDRESS)),

        // Asynchronous Command Processing
        PQsendQuery(of(JAVA_INT, ADDRESS, ADDRESS)),
        PQsendPrepare(of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS)),
        PQsendQueryPrepared(of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT)),
        PQsendDescribePrepared(of(JAVA_INT, ADDRESS, ADDRESS)),
        PQgetResult(of(ADDRESS, ADDRESS)),
        PQconsumeInput(of(JAVA_INT, ADDRESS)),
        PQisBusy(of(JAVA_INT, ADDRESS)),

        // Canceling Queries In Progress
        PQgetCancel(of(ADDRESS, ADDRESS)),
        PQfreeCancel(ofVoid(ADDRESS)),
        PQcancel(of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT)),

        // Miscellaneous Functions
        PQconninfoFree(ofVoid(ADDRESS));

        public final FunctionDescriptor fd;

        FUNCTION(
                final FunctionDescriptor fd) {

            this.fd = fd;
        }
    }
}
