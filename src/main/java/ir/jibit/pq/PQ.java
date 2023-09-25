package ir.jibit.pq;

import ir.jibit.pq.enums.ConnStatusType;
import ir.jibit.pq.enums.PGPing;
import ir.jibit.pq.enums.PGTransactionStatusType;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Optional;

import static ir.jibit.pq.Layouts.*;
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
public final class PQ implements AutoCloseable {

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
    private final MethodHandle execParamsHandle;

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
        this.execParamsHandle = linker.downcallHandle(lib.find(FUNCTION.PQexecParams.name()).orElseThrow(), FUNCTION.PQexecParams.fd);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">More info</a>
     */
    public MemorySegment connectDB(final String connInfo) throws Throwable {
        try (final var arena = Arena.ofConfined()) {
            return (MemorySegment) connectDBHandle.invokeExact(arena.allocateUtf8String(connInfo));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">More info</a>
     */
    public Optional<MemorySegment> connectDBOptional(final String connInfo) throws Throwable {
        final var pgConn = connectDB(connInfo);
        if (!pgConn.equals(NULL)) {
            return Optional.of(pgConn);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNINFO">More info</a>
     */
    public MemorySegment connInfo(final MemorySegment pgConn) throws Throwable {
        return (MemorySegment) connInfoHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNINFO">More info</a>
     */
    public Optional<MemorySegment> connInfoOptional(final MemorySegment pgConn) throws Throwable {
        final var connInfo = connInfo(pgConn);
        if (!connInfo.equals(NULL)) {
            return Optional.of(connInfo);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQFINISH">More info</a>
     */
    public void finish(final MemorySegment pgConn) throws Throwable {
        finishHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQRESET">More info</a>
     */
    public void reset(final MemorySegment pgConn) throws Throwable {
        resetHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQPING">More info</a>
     */
    public PGPing ping(final String connInfo) throws Throwable {
        try (final var arena = Arena.ofConfined()) {
            switch ((int) pingHandle.invokeExact(arena.allocateUtf8String(connInfo))) {
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
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-misc.html#LIBPQ-PQCONNINFOFREE">More info</a>
     */
    public void connInfoFree(final MemorySegment connIfo) throws Throwable {
        connInfoFreeHandle.invokeExact(connIfo);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQDB">More info</a>
     */
    public MemorySegment db(final MemorySegment pgConn) throws Throwable {
        return (MemorySegment) dbHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSTATUS">More info</a>
     */
    public ConnStatusType status(final MemorySegment pgConn) throws Throwable {
        switch ((int) statusHandle.invokeExact(pgConn)) {
            case 0:
                return ConnStatusType.CONNECTION_OK;
            case 1:
                return ConnStatusType.CONNECTION_BAD;

            default:
                return ConnStatusType.UNKNOWN;
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQTRANSACTIONSTATUS">More info</a>
     */
    public PGTransactionStatusType transactionStatus(final MemorySegment pgConn) throws Throwable {
        switch ((int) transactionStatusHandle.invokeExact(pgConn)) {
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
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQPROTOCOLVERSION">More info</a>
     */
    public int protocolVersion(final MemorySegment pgConn) throws Throwable {
        return (int) protocolVersionHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSERVERVERSION">More info</a>
     */
    public int serverVersion(final MemorySegment pgConn) throws Throwable {
        return (int) serverVersionHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQERRORMESSAGE">More info</a>
     */
    public MemorySegment errorMessage(final MemorySegment pgConn) throws Throwable {
        return (MemorySegment) errorMessageHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">More info</a>
     */
    public int socket(final MemorySegment pgConn) throws Throwable {
        return (int) socketHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">More info</a>
     */
    public Optional<Integer> socketOptional(final MemorySegment pgConn) throws Throwable {
        final var socket = (int) socketHandle.invokeExact(pgConn);
        if (socket > 0) {
            return Optional.of(socket);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQBACKENDPID">More info</a>
     */
    public int backendPid(final MemorySegment pgConn) throws Throwable {
        return (int) backendPidHandle.invokeExact(pgConn);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">More info</a>
     */
    public MemorySegment exec(final MemorySegment pgConn, final String query) throws Throwable {
        try (final var arena = Arena.ofConfined()) {
            return (MemorySegment) execHandle.invokeExact(pgConn, arena.allocateUtf8String(query));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">More info</a>
     */
    public MemorySegment exec(final MemorySegment pgConn, final MemorySegment query) throws Throwable {
        return (MemorySegment) execHandle.invokeExact(pgConn, query);
    }

    /**
     * Prints information about a connection to postgresql server.
     *
     * @param pgConn memory segment instance returned by connecting to postgresql server
     * @throws Throwable in case of errors
     */
    public void printConnInfo(final MemorySegment pgConn) throws Throwable {
        final var ptr = connInfoOptional(pgConn).orElseThrow();

        try {
            for (int i = 0; ; i++) {
                final var rPtr = ptr.reinterpret(PQConnInfoOption.byteSize() + PQConnInfoOption.byteSize() * i);
                final var keywordPtr = (MemorySegment) PQConnInfoOptionSequence_keyword_varHandle.get(rPtr, i);
                final var valPtr = (MemorySegment) PQConnInfoOptionSequence_val_varHandle.get(rPtr, i);

                if (keywordPtr.equals(NULL)) {
                    break;
                } else {
                    System.out.print(keywordPtr.reinterpret(256).getUtf8String(0) + ": ");
                    if (!valPtr.equals(NULL)) {
                        System.out.print(valPtr.reinterpret(256).getUtf8String(0));
                    }

                    System.out.println();
                }
            }
        } finally {
            connInfoFree(ptr);
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
        PQexecParams(FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, JAVA_INT));

        public final FunctionDescriptor fd;

        FUNCTION(FunctionDescriptor fd) {
            this.fd = fd;
        }
    }
}
