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

package software.openex.pq.cp;

import software.openex.pq.PQX;
import software.openex.pq.cp.xact.AccessMode;
import software.openex.pq.cp.xact.DeferrableMode;
import software.openex.pq.cp.xact.IsolationLevel;
import software.openex.pq.cp.xact.TransactionBlock;
import software.openex.pq.layout.PreparedStatement;
import software.openex.pq.type.ConnStatusType;
import software.openex.pq.type.ExecStatusType;
import software.openex.pq.type.FieldFormat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.lang.Thread.sleep;
import static java.lang.Thread.startVirtualThread;
import static java.lang.foreign.Arena.ofConfined;
import static java.lang.foreign.Arena.ofShared;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Logger.getLogger;
import static java.util.stream.IntStream.range;
import static software.openex.pq.std.CString.strlen;

/**
 * A connection pool implementation using {@link PQX}.
 *
 * @author Alireza Pourtaghi
 */
public class PQCP implements Configurable, AutoCloseable {
    private static final Logger logger = getLogger(PQCP.class.getName());

    private final ScheduledExecutorService connectionsStatusCheckerExecutor;
    private final int minPoolSize;
    private final int maxPoolSize;
    private final AtomicInteger poolSize;
    private final Duration connectTimeout;
    private final AtomicInteger notAvailableConnectionCounter;
    private final int makeNewConnectionCoefficient;
    private final Duration checkConnectionsStatusPeriod;

    private final PQX pqx;
    private final String connInfo;
    private final MemorySegment[] connections;
    private final Semaphore[] locks;

    private final Arena arena;
    private final ArrayList<PreparedStatement> preparedStatements;

    public PQCP(
            final Path path,
            final String connInfo) throws Exception {

        this(path,
                connInfo,
                DEFAULT_MIN_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD);
    }

    public PQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD);
    }

    public PQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                maxPoolSize,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD);
    }

    public PQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final Duration connectTimeout) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                maxPoolSize,
                connectTimeout,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD);
    }

    public PQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final Duration connectTimeout,
            final int makeNewConnectionCoefficient) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                maxPoolSize,
                connectTimeout,
                makeNewConnectionCoefficient,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD);
    }

    public PQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final Duration connectTimeout,
            final int makeNewConnectionCoefficient,
            final Duration checkConnectionsStatusPeriod) throws Exception {

        if (minPoolSize > maxPoolSize) {
            throw new IllegalArgumentException("minPoolSize > maxPoolSize");
        }

        if (makeNewConnectionCoefficient < 0) {
            throw new IllegalArgumentException("makeNewConnectionCoefficient is negative");
        }

        this.connectionsStatusCheckerExecutor = newSingleThreadScheduledExecutor();
        this.minPoolSize = minPoolSize > 0 ? minPoolSize : DEFAULT_MIN_POOL_SIZE;
        this.maxPoolSize = maxPoolSize > 0 ? maxPoolSize : DEFAULT_MAX_POOL_SIZE;
        this.poolSize = new AtomicInteger(minPoolSize);
        this.connectTimeout = connectTimeout != null ? connectTimeout : DEFAULT_CONNECT_TIMEOUT;
        this.notAvailableConnectionCounter = new AtomicInteger(0);
        this.makeNewConnectionCoefficient = makeNewConnectionCoefficient > 0 ? makeNewConnectionCoefficient : DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT;
        this.checkConnectionsStatusPeriod = checkConnectionsStatusPeriod != null ? checkConnectionsStatusPeriod : DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD;

        this.pqx = new PQX(path);
        this.connInfo = connInfo;
        this.connections = new MemorySegment[this.maxPoolSize];
        this.locks = new Semaphore[this.maxPoolSize];

        this.arena = ofShared();
        this.preparedStatements = new ArrayList<>();

        if (!connect(this)) {
            throw new Exception("could not build connection pool successfully!");
        }

        logBasicServerInfo(this);
        scheduleConnectionsStatusChecker(this);
    }

    public void prepare(
            final PreparedStatement preparedStatement) {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var query = (MemorySegment) preparedStatement.var("query").get(preparedStatement.getSegment());
        for (int i = 0; i < maxPoolSize; i++) {
            if (locks[i] != null) {
                try {
                    locks[i].acquire();
                    try {
                        prepareCached(connections[i], preparedStatement, stmtName, query, i == 0);
                    } finally {
                        locks[i].release();
                    }
                } catch (Throwable th) {
                    throw new RuntimeException(th);
                }
            }
        }
    }

    private void prepareCached(
            final MemorySegment conn,
            final PreparedStatement preparedStatement,
            final MemorySegment stmtName,
            final MemorySegment query,
            final boolean cache) throws Throwable {

        if (cache) {
            final var ps = new PreparedStatement(arena);
            ps.setStmtName(stmtName.reinterpret(strlen(stmtName) + 1).getString(0));
            ps.setQuery(query.reinterpret(strlen(query) + 1).getString(0));
            ps.setNParams((int) preparedStatement.var("nParams").get(preparedStatement.getSegment()));
            preparedStatements.add(ps);
        }

        prepare(conn, preparedStatement, stmtName);
    }

    private void prepare(
            final MemorySegment conn,
            final PreparedStatement preparedStatement,
            final MemorySegment stmtName) throws Throwable {

        var res = pqx.describePrepared(conn, stmtName);
        if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
            // Preparing statement ...
            res = pqx.prepare(conn, preparedStatement);
            if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                throw new RuntimeException(pqx.resultErrorMessageString(res));
            }
        }
    }

    public int execute(
            final PreparedStatement preparedStatement) throws TimeoutException {

        final var availableIndex = getAvailableConnectionIndexLocked(true, nanoTime(), 1);
        final var conn = connections[availableIndex];
        var connReleased = false;

        try {
            final var res = pqx.execPreparedBinaryResult(conn, preparedStatement);
            try {
                final var status = pqx.resultStatus(res);
                if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                    // Release as soon as possible.
                    locks[availableIndex].release();
                    connReleased = true;

                    return pqx.cmdTuplesInt(res);
                } else {
                    throw new RuntimeException(pqx.resultErrorMessageString(res));
                }
            } finally {
                pqx.clear(res);
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            if (!connReleased) locks[availableIndex].release();
        }
    }

    public int prepareThenExecute(
            final PreparedStatement preparedStatement) throws TimeoutException {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var availableIndex = getAvailableConnectionIndexLocked(true, nanoTime(), 1);
        final var conn = connections[availableIndex];
        var connReleased = false;

        try {
            prepare(conn, preparedStatement, stmtName);
            final var res = pqx.execPreparedBinaryResult(conn, preparedStatement);
            try {
                final var status = pqx.resultStatus(res);
                if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                    // Release as soon as possible.
                    locks[availableIndex].release();
                    connReleased = true;

                    return pqx.cmdTuplesInt(res);
                } else {
                    throw new RuntimeException(pqx.resultErrorMessageString(res));
                }
            } finally {
                pqx.clear(res);
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            if (!connReleased) locks[availableIndex].release();
        }
    }

    public int execute(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement) {

        checkTransactionBlockSafety(transactionBlock);
        try {
            final var res = pqx.execPreparedBinaryResult(transactionBlock.getConn(), preparedStatement);
            try {
                final var status = pqx.resultStatus(res);
                if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                    return pqx.cmdTuplesInt(res);
                } else {
                    throw new RuntimeException(pqx.resultErrorMessageString(res));
                }
            } finally {
                pqx.clear(res);
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    private void checkTransactionBlockSafety(
            final TransactionBlock transactionBlock) {

        if (transactionBlock.getDone().compareAndSet(true, true)) {
            throw new RuntimeException("transaction done: previously committed or rolled back");
        }
    }

    public int prepareThenExecute(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement) {

        checkTransactionBlockSafety(transactionBlock);
        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());

        try {
            prepare(transactionBlock.getConn(), preparedStatement, stmtName);
            final var res = pqx.execPreparedBinaryResult(transactionBlock.getConn(), preparedStatement);
            try {
                final var status = pqx.resultStatus(res);
                if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                    return pqx.cmdTuplesInt(res);
                } else {
                    throw new RuntimeException(pqx.resultErrorMessageString(res));
                }
            } finally {
                pqx.clear(res);
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public MemorySegment fetchTextResult(
            final PreparedStatement preparedStatement) throws TimeoutException {

        return fetch(preparedStatement, true);
    }

    public MemorySegment prepareThenFetchTextResult(
            final PreparedStatement preparedStatement) throws TimeoutException {

        return prepareThenFetch(preparedStatement, true);
    }

    public MemorySegment fetchTextResult(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement) {

        return fetch(transactionBlock, preparedStatement, true);
    }

    public MemorySegment prepareThenFetchTextResult(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement) {

        return prepareThenFetch(transactionBlock, preparedStatement, true);
    }

    public MemorySegment fetchBinaryResult(
            final PreparedStatement preparedStatement) throws TimeoutException {

        return fetch(preparedStatement, false);
    }

    public MemorySegment prepareThenFetchBinaryResult(
            final PreparedStatement preparedStatement) throws TimeoutException {

        return prepareThenFetch(preparedStatement, false);
    }

    public MemorySegment fetchBinaryResult(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement) {

        return fetch(transactionBlock, preparedStatement, false);
    }

    public MemorySegment prepareThenFetchBinaryResult(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement) {

        return prepareThenFetch(transactionBlock, preparedStatement, false);
    }

    private MemorySegment fetch(
            final PreparedStatement preparedStatement,
            final boolean text) throws TimeoutException {

        final var availableIndex = getAvailableConnectionIndexLocked(true, nanoTime(), 1);
        final var conn = connections[availableIndex];
        var connReleased = false;

        try {
            final var res = text ?
                    pqx.execPreparedTextResult(conn, preparedStatement) :
                    pqx.execPreparedBinaryResult(conn, preparedStatement);

            final var status = pqx.resultStatus(res);
            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                // Release as soon as possible.
                locks[availableIndex].release();
                connReleased = true;

                return res;
            } else {
                throw new RuntimeException(pqx.resultErrorMessageString(res));
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            if (!connReleased) locks[availableIndex].release();
        }
    }

    private MemorySegment prepareThenFetch(
            final PreparedStatement preparedStatement,
            final boolean text) throws TimeoutException {

        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
        final var availableIndex = getAvailableConnectionIndexLocked(true, nanoTime(), 1);
        final var conn = connections[availableIndex];
        var connReleased = false;

        try {
            prepare(conn, preparedStatement, stmtName);
            final var res = text ?
                    pqx.execPreparedTextResult(conn, preparedStatement) :
                    pqx.execPreparedBinaryResult(conn, preparedStatement);

            final var status = pqx.resultStatus(res);
            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                // Release as soon as possible.
                locks[availableIndex].release();
                connReleased = true;

                return res;
            } else {
                throw new RuntimeException(pqx.resultErrorMessageString(res));
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            if (!connReleased) locks[availableIndex].release();
        }
    }

    private MemorySegment fetch(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement,
            final boolean text) {

        checkTransactionBlockSafety(transactionBlock);
        try {
            final var res = text ?
                    pqx.execPreparedTextResult(transactionBlock.getConn(), preparedStatement) :
                    pqx.execPreparedBinaryResult(transactionBlock.getConn(), preparedStatement);

            final var status = pqx.resultStatus(res);
            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                return res;
            } else {
                throw new RuntimeException(pqx.resultErrorMessageString(res));
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    private MemorySegment prepareThenFetch(
            final TransactionBlock transactionBlock,
            final PreparedStatement preparedStatement,
            final boolean text) {

        checkTransactionBlockSafety(transactionBlock);
        final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());

        try {
            prepare(transactionBlock.getConn(), preparedStatement, stmtName);
            final var res = text ?
                    pqx.execPreparedTextResult(transactionBlock.getConn(), preparedStatement) :
                    pqx.execPreparedBinaryResult(transactionBlock.getConn(), preparedStatement);

            final var status = pqx.resultStatus(res);
            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                return res;
            } else {
                throw new RuntimeException(pqx.resultErrorMessageString(res));
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public TransactionBlock begin() throws TimeoutException {
        return begin(IsolationLevel.NONE, AccessMode.NONE, DeferrableMode.NONE);
    }

    public TransactionBlock begin(
            final IsolationLevel isolationLevel,
            final AccessMode accessMode,
            final DeferrableMode deferrableMode) throws TimeoutException {

        final var query = beginQuery(isolationLevel, accessMode, deferrableMode);
        final var availableIndex = getAvailableConnectionIndexLocked(true, nanoTime(), 1);
        try {
            final var res = pqx.exec(connections[availableIndex], query);
            final var status = pqx.resultStatus(res);

            if (status == ExecStatusType.PGRES_COMMAND_OK) {
                return new TransactionBlock(availableIndex, connections[availableIndex]);
            } else {
                throw new RuntimeException(format("status returned by database server was %s", status));
            }
        } catch (Throwable th) {
            locks[availableIndex].release();
            throw new RuntimeException(th);
        }
    }

    private String beginQuery(
            final IsolationLevel isolationLevel,
            final AccessMode accessMode,
            final DeferrableMode deferrableMode) {

        var query = format("BEGIN%s%s%s;",
                isolationLevel != IsolationLevel.NONE ? format(" %s,", isolationLevel.getValue()) : "",
                accessMode != AccessMode.NONE ? format(" %s,", accessMode.getValue()) : "",
                deferrableMode != DeferrableMode.NONE ? format(" %s", deferrableMode.getValue()) : "");

        if (query.charAt(query.length() - 2) == ',') {
            query = query.substring(0, query.length() - 2);
            query = query + ";";
        }

        return query;
    }

    public void commit(
            final TransactionBlock transactionBlock) {

        checkTransactionBlockSafety(transactionBlock);
        try {
            final var res = pqx.exec(transactionBlock.getConn(), "COMMIT;");
            final var status = pqx.resultStatus(res);

            if (status != ExecStatusType.PGRES_COMMAND_OK) {
                throw new RuntimeException(format("status returned by database server was %s", status));
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }

        // If command was ok.
        transactionBlock.getDone().compareAndSet(false, true);
        locks[transactionBlock.getIndex()].release();
    }

    public void rollback(
            final TransactionBlock transactionBlock) {

        checkTransactionBlockSafety(transactionBlock);
        try {
            final var res = pqx.exec(transactionBlock.getConn(), "ROLLBACK;");
            final var status = pqx.resultStatus(res);

            if (status != ExecStatusType.PGRES_COMMAND_OK) {
                throw new RuntimeException(format("status returned by database server was %s", status));
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }

        // If command was ok.
        transactionBlock.getDone().compareAndSet(false, true);
        locks[transactionBlock.getIndex()].release();
    }

    public void clear(
            final MemorySegment res) {

        try {
            pqx.clear(res);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public int nTuples(
            final MemorySegment res) {

        try {
            return pqx.nTuples(res);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public int nFields(
            final MemorySegment res) {

        try {
            return pqx.nFields(res);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public Optional<String> fNameOptional(
            final MemorySegment res,
            final int columnNumber) {

        try {
            return pqx.fNameOptional(res, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public Optional<Integer> fNumberOptional(
            final MemorySegment res,
            final String columnName) {

        try {
            return pqx.fNumberOptional(res, columnName);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public FieldFormat fFormat(
            final MemorySegment res,
            final int columnNumber) {

        try {
            return pqx.fFormat(res, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public int fType(
            final MemorySegment res,
            final int columnNumber) {

        try {
            return pqx.fType(res, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public int fMod(
            final MemorySegment res,
            final int columnNumber) {

        try {
            return pqx.fMod(res, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public MemorySegment getValue(
            final MemorySegment res,
            final int rowNumber,
            final int columnNumber) {

        try {
            return pqx.getValue(res, rowNumber, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public boolean getIsNull(
            final MemorySegment res,
            final int rowNumber,
            final int columnNumber) {

        try {
            return pqx.getIsNull(res, rowNumber, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public int getLength(
            final MemorySegment res,
            final int rowNumber,
            final int columnNumber) {

        try {
            return pqx.getLength(res, rowNumber, columnNumber);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    public int cmdTuplesInt(
            final MemorySegment res) {

        try {
            return pqx.cmdTuplesInt(res);
        } catch (Throwable th) {
            throw new RuntimeException(th);
        }
    }

    protected int getAvailableConnectionIndexLocked(
            final boolean incrementNotAvailability,
            final long start,
            int tryCount) throws TimeoutException {

        if (nanoTime() - start >= connectTimeout.toNanos()) {
            throw new TimeoutException(format("timeout of %d ms occurred while getting connection from pool", connectTimeout.toMillis()));
        }

        try {
            for (int i = 0; i < maxPoolSize; i++) {
                if (locks[i] != null && locks[i].tryAcquire()) {
                    return i;
                }

                // Let's check next in connections ...
            }

            if (incrementNotAvailability && notAvailableConnectionCounter.incrementAndGet() > poolSize.get() * makeNewConnectionCoefficient) {
                notAvailableConnectionCounter.getAndSet(0);

                // If not reached end of connections array size.
                if (!poolSize.compareAndSet(maxPoolSize, maxPoolSize)) {
                    startVirtualThread(() -> createNewConnection(poolSize.getAndIncrement()));
                }
            }

            if (tryCount % maxPoolSize == 0) sleep(1);
        } catch (Exception ex) {
            // Do nothing!
        }

        // Let's try for next time to find any available connection ...
        return getAvailableConnectionIndexLocked(false, start, tryCount + 1);
    }

    private synchronized void createNewConnection(
            final int atIndex) {

        if (atIndex < maxPoolSize && locks[atIndex] == null) {
            try {
                final var newLock = new Semaphore(1);
                newLock.acquire();
                locks[atIndex] = newLock;
                try (final var arena = ofConfined()) {
                    connections[atIndex] = pqx.connectDB(arena.allocateFrom(connInfo));
                }

                if (pqx.status(connections[atIndex]) != ConnStatusType.CONNECTION_OK) {
                    // Releasing ...
                    pqx.finish(connections[atIndex]);
                    connections[atIndex] = null;
                    final var lock = locks[atIndex];
                    locks[atIndex] = null;
                    lock.release();
                    return;
                }

                // Not thread safe! What are side effects?
                preparedStatements.forEach(ps -> {
                    try {
                        prepare(connections[atIndex], ps);
                    } catch (Throwable e) {
                        // Do nothing. Keep created connection open.
                    }
                });

                logger.info(format("extended pool size to have %d connections to handle more queries", atIndex + 1));
            } catch (Throwable th) {
                // Releasing ...
                // We reach this section only in case of exception at pqx.connectDB(arena.allocateUtf8String(connInfo)).
                // So connections[atIndex] will be null.
                final var lock = locks[atIndex];
                locks[atIndex] = null;
                lock.release();
            } finally {
                if (locks[atIndex] != null) locks[atIndex].release();
            }
        }
    }

    private void prepare(
            final MemorySegment conn,
            final PreparedStatement preparedStatement) throws Throwable {

        var res = pqx.describePrepared(conn, (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment()));
        if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
            // Preparing statement ...
            res = pqx.prepare(conn, preparedStatement);
            if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                throw new RuntimeException(pqx.resultErrorMessageString(res));
            }
        }
    }

    private static boolean connect(
            final PQCP cp) throws Exception {

        final var arena = ofShared();
        final var connInfoMemorySegment = arena.allocateFrom(cp.connInfo);
        final var counter = new CountDownLatch(cp.minPoolSize);
        final var connected = new AtomicBoolean(true);

        range(0, cp.minPoolSize).forEach(i -> startVirtualThread(() -> {
            // To not continue making new connections when connected is false.
            // If connected is true, set it to true and execute if block; otherwise don't execute if block.
            if (connected.compareAndSet(true, true)) {
                try {
                    final var lock = new Semaphore(1);
                    lock.acquire();
                    cp.locks[i] = lock;
                    cp.connections[i] = cp.pqx.connectDB(connInfoMemorySegment);
                    if (cp.pqx.status(cp.connections[i]) != ConnStatusType.CONNECTION_OK) {
                        throw new Exception("could not connect to database server");
                    }
                } catch (Throwable th) {
                    connected.compareAndSet(true, false);
                } finally {
                    cp.locks[i].release();
                }
            }

            counter.countDown();
        }));

        counter.await();
        arena.close();
        if (!connected.get()) cp.close();

        return connected.get();
    }

    private static void logBasicServerInfo(
            final PQCP cp) {

        try {
            cp.locks[0].acquire();
            logger.info(format(
                    "connected to postgresql server: [server version: %d, protocol version: %d, db: %s]",
                    cp.pqx.serverVersion(cp.connections[0]),
                    cp.pqx.protocolVersion(cp.connections[0]),
                    cp.pqx.dbString(cp.connections[0])));
        } catch (Throwable th) {
            logger.warning("could not log server information!");
        } finally {
            cp.locks[0].release();
        }
    }

    private static void scheduleConnectionsStatusChecker(
            final PQCP cp) {

        cp.connectionsStatusCheckerExecutor.scheduleAtFixedRate(
                () -> checkConnectionsStatus(cp), 0L, cp.checkConnectionsStatusPeriod.toMillis(), MILLISECONDS);
    }

    private static void checkConnectionsStatus(
            final PQCP cp) {

        for (int conn = 0; conn < cp.maxPoolSize; conn++) {
            if (cp.connections[conn] != null) {
                try {
                    if (cp.pqx.status(cp.connections[conn]) == ConnStatusType.CONNECTION_BAD) {
                        logger.info("going to reset a bad connection ...");

                        // Blocks checkConnectionsStatusPeriod time for a connection to be available for resetting.
                        if (cp.locks[conn].tryAcquire(cp.checkConnectionsStatusPeriod.toMillis(), MILLISECONDS)) {
                            try {
                                cp.pqx.reset(cp.connections[conn]);
                            } finally {
                                cp.locks[conn].release();
                            }
                        }
                    }
                } catch (Throwable th) {
                    logger.warning(format("could not check connections status: %s", th.getMessage()));
                }
            }
        }
    }

    @Override
    public final void close() throws Exception {
        for (int conn = 0; conn < maxPoolSize; conn++) {
            if (connections[conn] != null) {
                try {
                    locks[conn].acquire();
                    pqx.finish(connections[conn]);
                } catch (Throwable th) {
                    // We must call finish for next connection.
                } finally {
                    locks[conn].release();
                }
            }
        }

        connectionsStatusCheckerExecutor.shutdown();
        pqx.close();
        arena.close();
    }

    public final int getMaxPoolSize() {
        return maxPoolSize;
    }

    public final PQX getPqx() {
        return pqx;
    }

    public final MemorySegment[] getConnections() {
        return connections;
    }

    public final Semaphore[] getLocks() {
        return locks;
    }
}
