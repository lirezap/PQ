package ir.jibit.pq;

import ir.jibit.pq.enums.PGPing;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Optional;

import static ir.jibit.pq.layouts.PQConnInfoOption.*;
import static ir.jibit.pq.layouts.PreparedStatement.*;
import static java.lang.foreign.MemorySegment.NULL;

/**
 * {@link PQ} extended to have some utility methods.
 *
 * @author Alireza Pourtaghi
 */
public final class PQX extends PQ {

    public PQX(Path path) {
        super(path);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQCONNECTDB">See official doc for more information.</a>
     */
    public Optional<MemorySegment> connectDB(final String connInfo) throws Throwable {
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
    public Optional<MemorySegment> connInfoOptional(final MemorySegment conn) throws Throwable {
        final var connInfo = connInfo(conn);
        if (!connInfo.equals(NULL)) {
            return Optional.of(connInfo);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-PQPING">See official doc for more information.</a>
     */
    public PGPing ping(final String connInfo) throws Throwable {
        try (final var arena = Arena.ofConfined()) {
            return ping(arena.allocateUtf8String(connInfo));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-status.html#LIBPQ-PQSOCKET">See official doc for more information.</a>
     */
    public Optional<Integer> socketOptional(final MemorySegment conn) throws Throwable {
        final var socket = socket(conn);
        if (socket > 0) {
            return Optional.of(socket);
        }

        return Optional.empty();
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQEXEC">See official doc for more information.</a>
     */
    public MemorySegment exec(final MemorySegment conn, final String command) throws Throwable {
        try (final var arena = Arena.ofConfined()) {
            return exec(conn, arena.allocateUtf8String(command));
        }
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQPREPARE">See official doc for more information.</a>
     */
    public MemorySegment prepare(final MemorySegment conn, final String stmtName, final String query,
                                 final int nParams) throws Throwable {

        try (final var arena = Arena.ofConfined()) {
            return prepare(conn, arena.allocateUtf8String(stmtName), arena.allocateUtf8String(query), nParams);
        }
    }

    /**
     * Executes a prepared statement by using pointers provided as struct fields in parameter.
     *
     * @param conn                 postgresql database connection
     * @param preparedStatementPtr pointer to an instance of {@link ir.jibit.pq.layouts.PreparedStatement} struct
     * @return pointer to a postgresql result model in text format
     */
    public MemorySegment execPreparedTextResult(final MemorySegment conn,
                                                final MemorySegment preparedStatementPtr) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatementPtr);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatementPtr);
        final var paramValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatementPtr);
        final var paramLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatementPtr);
        final var paramFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatementPtr);

        return execPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, 0);
    }

    /**
     * Executes a prepared statement by using pointers provided as struct fields in parameter.
     *
     * @param conn                 postgresql database connection
     * @param preparedStatementPtr pointer to an instance of {@link ir.jibit.pq.layouts.PreparedStatement} struct
     * @return pointer to a postgresql result model in binary format
     */
    public MemorySegment execPreparedBinaryResult(final MemorySegment conn,
                                                  final MemorySegment preparedStatementPtr) throws Throwable {

        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatementPtr);
        final var nParams = (int) PreparedStatement_nParams_varHandle.get(preparedStatementPtr);
        final var paramValues = (MemorySegment) PreparedStatement_paramValues_varHandle.get(preparedStatementPtr);
        final var paramLengths = (MemorySegment) PreparedStatement_paramLengths_varHandle.get(preparedStatementPtr);
        final var paramFormats = (MemorySegment) PreparedStatement_paramFormats_varHandle.get(preparedStatementPtr);

        return execPrepared(conn, stmtName, nParams, paramValues, paramLengths, paramFormats, 1);
    }

    /**
     * <a href="https://www.postgresql.org/docs/16/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE">See official doc for more information.</a>
     */
    public String resultErrorMessageString(final MemorySegment pgResult) throws Throwable {
        return resultErrorMessage(pgResult).reinterpret(1024).getUtf8String(0);
    }

    /**
     * Prints information about a connection to postgresql server.
     *
     * @param conn memory segment instance returned by connecting to postgresql server
     * @throws Throwable in case of errors
     */
    public void printConnInfo(final MemorySegment conn) throws Throwable {
        final var ptr = connInfoOptional(conn).orElseThrow();

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
}