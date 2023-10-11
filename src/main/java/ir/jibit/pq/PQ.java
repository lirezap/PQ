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

import ir.jibit.pq.enums.*;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.MemorySegment.NULL;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Java FFM used wrapper of postgresql C library.
 * <p>This library is intended to be low level, minimal and with the goal of being high performance.</p>
 * <p>Higher level abstractions can be built easily by using these low level functions.</p>
 *
 * @author Alireza Pourtaghi
 */
public sealed class PQ implements AutoCloseable permits PQX {

    /**
     * Shared or dynamic postgresql c library path.
     */
    private final Path path;

    /**
     * Memory region that is needed to load library into.
     */
    private final Arena memory;

    /**
     * Library symbols; including functions and variables.
     */
    private final SymbolLookup lib;

    /**
     * Native linker.
     */
    private final Linker linker;

    // Database Connection Control Functions
    private final MethodHandle connectDBHandle;
    private final MethodHandle connInfoHandle;
    private final MethodHandle finishHandle;
    private final MethodHandle resetHandle;
    private final MethodHandle pingHandle;
    private final MethodHandle connInfoFreeHandle;

    // Connection Status Functions
    private final MethodHandle dbHandle;
    private final MethodHandle statusHandle;
    private final MethodHandle transactionStatusHandle;
    private final MethodHandle protocolVersionHandle;
    private final MethodHandle serverVersionHandle;
    private final MethodHandle errorMessageHandle;
    private final MethodHandle socketHandle;
    private final MethodHandle backendPidHandle;

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

    public PQ(final Path path) {
        this.path = path;
        this.memory = Arena.ofShared();
        this.lib = SymbolLookup.libraryLookup(path, memory);
        this.linker = Linker.nativeLinker();

        // Database Connection Control Functions
        this.connectDBHandle = linker.downcallHandle(lib.find(FUNCTION.PQconnectdb.name()).orElseThrow(), FUNCTION.PQconnectdb.fd);
        this.connInfoHandle = linker.downcallHandle(lib.find(FUNCTION.PQconninfo.name()).orElseThrow(), FUNCTION.PQconninfo.fd);
        this.finishHandle = linker.downcallHandle(lib.find(FUNCTION.PQfinish.name()).orElseThrow(), FUNCTION.PQfinish.fd);
        this.resetHandle = linker.downcallHandle(lib.find(FUNCTION.PQreset.name()).orElseThrow(), FUNCTION.PQreset.fd);
        this.pingHandle = linker.downcallHandle(lib.find(FUNCTION.PQping.name()).orElseThrow(), FUNCTION.PQping.fd);
        this.connInfoFreeHandle = linker.downcallHandle(lib.find(FUNCTION.PQconninfoFree.name()).orElseThrow(), FUNCTION.PQconninfoFree.fd);

        // Connection Status Functions
        this.dbHandle = linker.downcallHandle(lib.find(FUNCTION.PQdb.name()).orElseThrow(), FUNCTION.PQdb.fd);
        this.statusHandle = linker.downcallHandle(lib.find(FUNCTION.PQstatus.name()).orElseThrow(), FUNCTION.PQstatus.fd);
        this.transactionStatusHandle = linker.downcallHandle(lib.find(FUNCTION.PQtransactionStatus.name()).orElseThrow(), FUNCTION.PQtransactionStatus.fd);
        this.protocolVersionHandle = linker.downcallHandle(lib.find(FUNCTION.PQprotocolVersion.name()).orElseThrow(), FUNCTION.PQprotocolVersion.fd);
        this.serverVersionHandle = linker.downcallHandle(lib.find(FUNCTION.PQserverVersion.name()).orElseThrow(), FUNCTION.PQserverVersion.fd);
        this.errorMessageHandle = linker.downcallHandle(lib.find(FUNCTION.PQerrorMessage.name()).orElseThrow(), FUNCTION.PQerrorMessage.fd);
        this.socketHandle = linker.downcallHandle(lib.find(FUNCTION.PQsocket.name()).orElseThrow(), FUNCTION.PQsocket.fd);
        this.backendPidHandle = linker.downcallHandle(lib.find(FUNCTION.PQbackendPID.name()).orElseThrow(), FUNCTION.PQbackendPID.fd);

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
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">See official doc for more information.</a>
     */
    public MemorySegment connectDB(final MemorySegment connInfo) throws Throwable {
        return (MemorySegment) connectDBHandle.invokeExact(connInfo);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNINFO">See official doc for more information.</a>
     */
    public MemorySegment connInfo(final MemorySegment conn) throws Throwable {
        return (MemorySegment) connInfoHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQFINISH">See official doc for more information.</a>
     */
    public void finish(final MemorySegment conn) throws Throwable {
        finishHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQRESET">See official doc for more information.</a>
     */
    public void reset(final MemorySegment conn) throws Throwable {
        resetHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQPING">See official doc for more information.</a>
     */
    public PGPing ping(final MemorySegment connInfo) throws Throwable {
        switch ((int) pingHandle.invokeExact(connInfo)) {
            case 0:
                return PGPing.PQPING_OK;
            case 1:
                return PGPing.PQPING_REJECT;
            case 2:
                return PGPing.PQPING_NO_RESPONSE;
            case 3:
                return PGPing.PQPING_NO_ATTEMPT;

            default:
                return PGPing.UNKNOWN;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-misc.html#LIBPQ-PQCONNINFOFREE">See official doc for more information.</a>
     */
    public void connInfoFree(final MemorySegment connOptions) throws Throwable {
        connInfoFreeHandle.invokeExact(connOptions);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQDB">See official doc for more information.</a>
     */
    public MemorySegment db(final MemorySegment conn) throws Throwable {
        return (MemorySegment) dbHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSTATUS">See official doc for more information.</a>
     */
    public ConnStatusType status(final MemorySegment conn) throws Throwable {
        switch ((int) statusHandle.invokeExact(conn)) {
            case 0:
                return ConnStatusType.CONNECTION_OK;
            case 1:
                return ConnStatusType.CONNECTION_BAD;

            default:
                return ConnStatusType.UNKNOWN;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQTRANSACTIONSTATUS">See official doc for more information.</a>
     */
    public PGTransactionStatusType transactionStatus(final MemorySegment conn) throws Throwable {
        switch ((int) transactionStatusHandle.invokeExact(conn)) {
            case 0:
                return PGTransactionStatusType.PQTRANS_IDLE;
            case 1:
                return PGTransactionStatusType.PQTRANS_ACTIVE;
            case 2:
                return PGTransactionStatusType.PQTRANS_INTRANS;
            case 3:
                return PGTransactionStatusType.PQTRANS_INERROR;
            case 4:
                return PGTransactionStatusType.PQTRANS_UNKNOWN;

            default:
                return PGTransactionStatusType.UNKNOWN;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQPROTOCOLVERSION">See official doc for more information.</a>
     */
    public int protocolVersion(final MemorySegment conn) throws Throwable {
        return (int) protocolVersionHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSERVERVERSION">See official doc for more information.</a>
     */
    public int serverVersion(final MemorySegment conn) throws Throwable {
        return (int) serverVersionHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQERRORMESSAGE">See official doc for more information.</a>
     */
    public MemorySegment errorMessage(final MemorySegment conn) throws Throwable {
        return (MemorySegment) errorMessageHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">See official doc for more information.</a>
     */
    public int socket(final MemorySegment conn) throws Throwable {
        return (int) socketHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQBACKENDPID">See official doc for more information.</a>
     */
    public int backendPid(final MemorySegment conn) throws Throwable {
        return (int) backendPidHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">See official doc for more information.</a>
     */
    public MemorySegment exec(final MemorySegment conn, final MemorySegment command) throws Throwable {
        return (MemorySegment) execHandle.invokeExact(conn, command);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQPREPARE">See official doc for more information.</a>
     */
    public MemorySegment prepare(final MemorySegment conn, final MemorySegment stmtName, final MemorySegment query,
                                 final int nParams) throws Throwable {

        return (MemorySegment) prepareHandle.invokeExact(conn, stmtName, query, nParams, NULL);
    }

    /**
     * <a href="https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQEXECPREPARED">See official doc for more information.</a>
     */
    public MemorySegment execPrepared(final MemorySegment conn, final MemorySegment stmtName, final int nParams,
                                      final MemorySegment paramValues, final MemorySegment paramLengths,
                                      final MemorySegment paramFormats, final int resultFormat) throws Throwable {

        return (MemorySegment) execPreparedHandle.invokeExact(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQDESCRIBEPREPARED">See official doc for more information.</a>
     */
    public MemorySegment describePrepared(final MemorySegment conn, final MemorySegment stmtName) throws Throwable {
        return (MemorySegment) describePreparedHandle.invokeExact(conn, stmtName);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTSTATUS">See official doc for more information.</a>
     */
    public ExecStatusType resultStatus(final MemorySegment res) throws Throwable {
        switch ((int) resultStatusHandle.invokeExact(res)) {
            case 0:
                return ExecStatusType.PGRES_EMPTY_QUERY;
            case 1:
                return ExecStatusType.PGRES_COMMAND_OK;
            case 2:
                return ExecStatusType.PGRES_TUPLES_OK;
            case 3:
                return ExecStatusType.PGRES_COPY_OUT;
            case 4:
                return ExecStatusType.PGRES_COPY_IN;
            case 5:
                return ExecStatusType.PGRES_BAD_RESPONSE;
            case 6:
                return ExecStatusType.PGRES_NONFATAL_ERROR;
            case 7:
                return ExecStatusType.PGRES_FATAL_ERROR;
            case 8:
                return ExecStatusType.PGRES_COPY_BOTH;
            case 9:
                return ExecStatusType.PGRES_SINGLE_TUPLE;
            case 10:
                return ExecStatusType.PGRES_PIPELINE_SYNC;
            case 11:
                return ExecStatusType.PGRES_PIPELINE_ABORTED;

            default:
                return ExecStatusType.UNKNOWN;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE">See official doc for more information.</a>
     */
    public MemorySegment resultErrorMessage(final MemorySegment res) throws Throwable {
        return (MemorySegment) resultErrorMessageHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCLEAR">See official doc for more information.</a>
     */
    public void clear(final MemorySegment res) throws Throwable {
        clearHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQNTUPLES">See official doc for more information.</a>
     */
    public int nTuples(final MemorySegment res) throws Throwable {
        return (int) nTuplesHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQNFIELDS">See official doc for more information.</a>
     */
    public int nFields(final MemorySegment res) throws Throwable {
        return (int) nFieldsHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNAME">See official doc for more information.</a>
     */
    public MemorySegment fName(final MemorySegment res, final int columnNumber) throws Throwable {
        return (MemorySegment) fNameHandle.invokeExact(res, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFNUMBER">See official doc for more information.</a>
     */
    public int fNumber(final MemorySegment res, final MemorySegment columnName) throws Throwable {
        return (int) fNumberHandle.invokeExact(res, columnName);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFFORMAT">See official doc for more information.</a>
     */
    public FieldFormat fFormat(final MemorySegment res, final int columnNumber) throws Throwable {
        switch ((int) fFormatHandle.invokeExact(res, columnNumber)) {
            case 0:
                return FieldFormat.TEXT;
            case 1:
                return FieldFormat.BINARY;

            default:
                return FieldFormat.UNKNOWN;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFTYPE">See official doc for more information.</a>
     */
    public int fType(final MemorySegment res, final int columnNumber) throws Throwable {
        // TODO: We need to return enum type here instead of int.
        return (int) fTypeHandle.invokeExact(res, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQFMOD">See official doc for more information.</a>
     */
    public int fMod(final MemorySegment res, final int columnNumber) throws Throwable {
        return (int) fModHandle.invokeExact(res, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQGETVALUE">See official doc for more information.</a>
     */
    public MemorySegment getValue(final MemorySegment res, final int rowNumber, final int columnNumber) throws Throwable {
        return (MemorySegment) getValueHandle.invokeExact(res, rowNumber, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQGETISNULL">See official doc for more information.</a>
     */
    public boolean getIsNull(final MemorySegment res, final int rowNumber, final int columnNumber) throws Throwable {
        switch ((int) getIsNullHandle.invokeExact(res, rowNumber, columnNumber)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQGETLENGTH">See official doc for more information.</a>
     */
    public int getLength(final MemorySegment res, final int rowNumber, final int columnNumber) throws Throwable {
        return (int) getLengthHandle.invokeExact(res, rowNumber, columnNumber);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCMDTUPLES">See official doc for more information.</a>
     */
    public MemorySegment cmdTuples(final MemorySegment res) throws Throwable {
        return (MemorySegment) cmdTuplesHandle.invokeExact(res);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDQUERY">See official doc for more information.</a>
     */
    public boolean sendQuery(final MemorySegment conn, final MemorySegment command) throws Throwable {
        switch ((int) sendQueryHandle.invokeExact(conn, command)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDPREPARE">See official doc for more information.</a>
     */
    public boolean sendPrepare(final MemorySegment conn, final MemorySegment stmtName, final MemorySegment query,
                               final int nParams) throws Throwable {

        switch ((int) sendPrepareHandle.invokeExact(conn, stmtName, query, nParams, NULL)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDQUERYPREPARED">See official doc for more information.</a>
     */
    public boolean sendQueryPrepared(final MemorySegment conn, final MemorySegment stmtName, final int nParams,
                                     final MemorySegment paramValues, final MemorySegment paramLengths,
                                     final MemorySegment paramFormats, final int resultFormat) throws Throwable {

        switch ((int) sendQueryPreparedHandle.invokeExact(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, resultFormat)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQSENDDESCRIBEPREPARED">See official doc for more information.</a>
     */
    public boolean sendDescribePrepared(final MemorySegment conn, final MemorySegment stmtName) throws Throwable {
        switch ((int) sendDescribePreparedHandle.invokeExact(conn, stmtName)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQGETRESULT">See official doc for more information.</a>
     */
    public MemorySegment getResult(final MemorySegment conn) throws Throwable {
        return (MemorySegment) getResultHandle.invokeExact(conn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQCONSUMEINPUT">See official doc for more information.</a>
     */
    public boolean consumeInput(final MemorySegment conn) throws Throwable {
        switch ((int) consumeInputHandle.invokeExact(conn)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-async.html#LIBPQ-PQISBUSY">See official doc for more information.</a>
     */
    public boolean isBusy(final MemorySegment conn) throws Throwable {
        switch ((int) isBusyHandle.invokeExact(conn)) {
            case 0:
                return false;
            default:
                return true;
        }
    }

    @Override
    public void close() throws Exception {
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
        PQconnectdb(FunctionDescriptor.of(ADDRESS, ADDRESS)),
        PQconninfo(FunctionDescriptor.of(ADDRESS, ADDRESS)),
        PQfinish(FunctionDescriptor.ofVoid(ADDRESS)),
        PQreset(FunctionDescriptor.ofVoid(ADDRESS)),
        PQping(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQconninfoFree(FunctionDescriptor.ofVoid(ADDRESS)),

        // Connection Status Functions
        PQdb(FunctionDescriptor.of(ADDRESS, ADDRESS)),
        PQstatus(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQtransactionStatus(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQprotocolVersion(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQserverVersion(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQerrorMessage(FunctionDescriptor.of(ADDRESS, ADDRESS)),
        PQsocket(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQbackendPID(FunctionDescriptor.of(JAVA_INT, ADDRESS)),

        // Command Execution Functions
        PQexec(FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS)),
        PQresultStatus(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQresultErrorMessage(FunctionDescriptor.of(ADDRESS, ADDRESS)),
        PQprepare(FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS)),
        PQexecPrepared(FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT)),
        PQdescribePrepared(FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS)),
        PQclear(FunctionDescriptor.ofVoid(ADDRESS)),
        PQntuples(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQnfields(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQfname(FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_INT)),
        PQfnumber(FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS)),
        PQfformat(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT)),
        PQftype(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT)),
        PQfmod(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT)),
        PQgetvalue(FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_INT, JAVA_INT)),
        PQgetisnull(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)),
        PQgetlength(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT)),
        PQcmdTuples(FunctionDescriptor.of(ADDRESS, ADDRESS)),

        // Asynchronous Command Processing
        PQsendQuery(FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS)),
        PQsendPrepare(FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS)),
        PQsendQueryPrepared(FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT)),
        PQsendDescribePrepared(FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS)),
        PQgetResult(FunctionDescriptor.of(ADDRESS, ADDRESS)),
        PQconsumeInput(FunctionDescriptor.of(JAVA_INT, ADDRESS)),
        PQisBusy(FunctionDescriptor.of(JAVA_INT, ADDRESS));

        public final FunctionDescriptor fd;

        FUNCTION(FunctionDescriptor fd) {
            this.fd = fd;
        }
    }
}
