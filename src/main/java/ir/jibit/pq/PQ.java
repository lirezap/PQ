package ir.jibit.pq;

import ir.jibit.pq.enums.ConnStatusType;
import ir.jibit.pq.enums.ExecStatusType;
import ir.jibit.pq.enums.PGPing;
import ir.jibit.pq.enums.PGTransactionStatusType;

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
    private final MethodHandle resultStatusHandle;
    private final MethodHandle resultErrorMessageHandle;
    private final MethodHandle clearHandle;

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
        this.resultStatusHandle = linker.downcallHandle(lib.find(FUNCTION.PQresultStatus.name()).orElseThrow(), FUNCTION.PQresultStatus.fd);
        this.resultErrorMessageHandle = linker.downcallHandle(lib.find(FUNCTION.PQresultErrorMessage.name()).orElseThrow(), FUNCTION.PQresultErrorMessage.fd);
        this.clearHandle = linker.downcallHandle(lib.find(FUNCTION.PQclear.name()).orElseThrow(), FUNCTION.PQclear.fd);
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
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTSTATUS">See official doc for more information.</a>
     */
    public ExecStatusType resultStatus(final MemorySegment pgResult) throws Throwable {
        switch ((int) resultStatusHandle.invokeExact(pgResult)) {
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
    public MemorySegment resultErrorMessage(final MemorySegment pgResult) throws Throwable {
        return (MemorySegment) resultErrorMessageHandle.invokeExact(pgResult);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQCLEAR">See official doc for more information.</a>
     */
    public void clear(final MemorySegment pgResult) throws Throwable {
        clearHandle.invokeExact(pgResult);
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
        PQclear(FunctionDescriptor.ofVoid(ADDRESS));

        public final FunctionDescriptor fd;

        FUNCTION(FunctionDescriptor fd) {
            this.fd = fd;
        }
    }
}
