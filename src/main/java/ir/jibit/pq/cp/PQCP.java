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

package ir.jibit.pq.cp;

import ir.jibit.pq.PQ;
import ir.jibit.pq.PQX;
import ir.jibit.pq.enums.ConnStatusType;
import ir.jibit.pq.enums.ExecStatusType;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static ir.jibit.pq.layouts.PreparedStatement.PreparedStatement_stmtName_varHandle;

/**
 * A connection pool implementation using {@link PQ}.
 *
 * @author Alireza Pourtaghi
 */
public class PQCP implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(PQCP.class.getName());

    private static final int DEFAULT_MIN_POOL_SIZE = 10;
    private static final int DEFAULT_MAX_POOL_SIZE = 25;
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final int DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT = 100;

    private final int minPoolSize;
    private final int maxPoolSize;
    private final AtomicInteger poolSize;
    private final Duration connectTimeout;
    private final AtomicInteger notAvailableConnectionCounter;
    private final int makeNewConnectionCoefficient;

    private final PQX pqx;
    private final String connInfo;
    private final MemorySegment[] connections;
    private final Semaphore[] locks;

    public PQCP(final Path path, final String connInfo) throws Exception {
        this(path, connInfo, DEFAULT_MIN_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize) throws Exception {
        this(path, connInfo, minPoolSize, DEFAULT_MAX_POOL_SIZE, DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize, final int maxPoolSize) throws Exception {
        this(path, connInfo, minPoolSize, maxPoolSize, DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize, final int maxPoolSize,
                final Duration connectTimeout) throws Exception {

        this(path, connInfo, minPoolSize, maxPoolSize, connectTimeout, DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize, final int maxPoolSize,
                final Duration connectTimeout, final int makeNewConnectionCoefficient) throws Exception {

        if (minPoolSize > maxPoolSize) {
            throw new IllegalArgumentException("minPoolSize > maxPoolSize");
        }

        if (makeNewConnectionCoefficient < 0) {
            throw new IllegalArgumentException("makeNewConnectionCoefficient is negative");
        }

        this.minPoolSize = minPoolSize > 0 ? minPoolSize : DEFAULT_MIN_POOL_SIZE;
        this.maxPoolSize = maxPoolSize > 0 ? maxPoolSize : DEFAULT_MAX_POOL_SIZE;
        this.poolSize = new AtomicInteger(minPoolSize);
        this.connectTimeout = connectTimeout != null ? connectTimeout : DEFAULT_CONNECT_TIMEOUT;
        this.notAvailableConnectionCounter = new AtomicInteger(0);
        this.makeNewConnectionCoefficient = makeNewConnectionCoefficient > 0 ? makeNewConnectionCoefficient : DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT;

        this.pqx = new PQX(path);
        this.connInfo = connInfo;
        this.connections = new MemorySegment[this.maxPoolSize];
        this.locks = new Semaphore[this.maxPoolSize];

        if (!connect(this)) {
            throw new Exception("could not build connection pool successfully!");
        }

        logBasicServerInfo(this);
    }

    public int prepareThenExecute(final MemorySegment preparedStatement) throws TimeoutException, RuntimeException {
        final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime());
        final var conn = connections[availableIndex];

        try {
            prepare(conn, preparedStatement);
            final var res = pqx.execPreparedBinaryResult(conn, preparedStatement);
            try {
                final var status = pqx.resultStatus(res);
                if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                    return pqx.cmdTuplesInt(res);
                } else {
                    throw new RuntimeException(String.format("status returned by database server was %s", status));
                }
            } finally {
                pqx.clear(res);
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            locks[availableIndex].release();
        }
    }

    public MemorySegment prepareThenFetchTextResult(final MemorySegment preparedStatement)
            throws TimeoutException, RuntimeException {

        return prepareThenFetch(preparedStatement, true);
    }

    public MemorySegment prepareThenFetchBinaryResult(final MemorySegment preparedStatement)
            throws TimeoutException, RuntimeException {

        return prepareThenFetch(preparedStatement, false);
    }

    public void clear(final MemorySegment res) throws Throwable {
        pqx.clear(res);
    }

    private MemorySegment prepareThenFetch(final MemorySegment preparedStatement, final boolean text)
            throws TimeoutException, RuntimeException {

        final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime());
        final var conn = connections[availableIndex];
        try {
            prepare(conn, preparedStatement);
            final var res = text ? pqx.execPreparedTextResult(conn, preparedStatement) : pqx.execPreparedBinaryResult(conn, preparedStatement);
            try {
                final var status = pqx.resultStatus(res);
                if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                    return res;
                } else {
                    throw new RuntimeException(String.format("status returned by database server was %s", status));
                }
            } finally {
                pqx.clear(res);
            }
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            locks[availableIndex].release();
        }
    }

    private void prepare(final MemorySegment conn, final MemorySegment preparedStatement) throws Throwable {
        final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
        var res = pqx.describePrepared(conn, stmtName);
        if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
            // Preparing statement ...
            res = pqx.prepare(conn, preparedStatement);
            if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                throw new RuntimeException(pqx.errorMessage(res).reinterpret(256).getUtf8String(0));
            }
        }
    }

    private int getAvailableConnectionIndexLocked(final boolean incrementNotAvailability, final long start) throws TimeoutException {
        if (System.nanoTime() - start >= connectTimeout.toNanos()) {
            throw new TimeoutException(String.format("timeout of %d ms occurred while getting connection from pool", connectTimeout.toMillis()));
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
                    Thread.startVirtualThread(() -> makeNewConnection(poolSize.getAndIncrement()));
                }
            }

            Thread.sleep(1);
        } catch (Exception ex) {
            // Do nothing!
        }

        // Let's try for next time to find any available connection ...
        return getAvailableConnectionIndexLocked(false, start);
    }

    private synchronized void makeNewConnection(final int atIndex) {
        if (atIndex < maxPoolSize && locks[atIndex] == null) {
            try {
                final var newLock = new Semaphore(1);
                newLock.acquire();
                locks[atIndex] = newLock;
                try (final var arena = Arena.ofConfined()) {
                    connections[atIndex] = pqx.connectDB(arena.allocateUtf8String(connInfo));
                }

                if (pqx.status(connections[atIndex]) != ConnStatusType.CONNECTION_OK) {
                    // Releasing ...
                    pqx.finish(connections[atIndex]);
                    connections[atIndex] = null;
                    final var lock = locks[atIndex];
                    locks[atIndex] = null;
                    lock.release();
                }

                logger.info(String.format("extended pool size to have %d connections to handle more queries", atIndex + 1));
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

    private static boolean connect(final PQCP cp) throws Exception {
        final var arena = Arena.ofShared();
        final var connInfoMemorySegment = arena.allocateUtf8String(cp.connInfo);
        final var counter = new CountDownLatch(cp.minPoolSize);
        final var connected = new AtomicBoolean(true);

        IntStream.range(0, cp.minPoolSize).forEach(i -> Thread.startVirtualThread(() -> {
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
        if (!connected.get()) {
            cp.close();
        }

        return connected.get();
    }

    private static void logBasicServerInfo(final PQCP cp) {
        try {
            cp.locks[0].acquire();

            logger.info(String.format(
                    "connected to postgresql server: [server version: %d, protocol version: %d, db: %s]",
                    cp.pqx.serverVersion(cp.connections[0]),
                    cp.pqx.protocolVersion(cp.connections[0]),
                    cp.pqx.db(cp.connections[0]).reinterpret(128).getUtf8String(0)));
        } catch (Throwable th) {
            logger.warning("could not log server information!");
        } finally {
            cp.locks[0].release();
        }
    }

    @Override
    public void close() throws Exception {
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

        pqx.close();
    }
}
