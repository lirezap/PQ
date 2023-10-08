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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.IntStream;

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

    private final int minPoolSize;
    private final int maxPoolSize;
    private final AtomicInteger poolSize;
    private final Duration connectTimeout;
    private final AtomicInteger notAvailableConnectionCounter;

    private final PQX pqx;
    private final String connInfo;
    private final MemorySegment[] connections;
    private final Semaphore[] locks;

    public PQCP(final Path path, final String connInfo) throws Exception {
        this(path, connInfo, DEFAULT_MIN_POOL_SIZE, DEFAULT_MAX_POOL_SIZE, DEFAULT_CONNECT_TIMEOUT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize) throws Exception {
        this(path, connInfo, minPoolSize, DEFAULT_MAX_POOL_SIZE, DEFAULT_CONNECT_TIMEOUT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize, final int maxPoolSize) throws Exception {
        this(path, connInfo, minPoolSize, maxPoolSize, DEFAULT_CONNECT_TIMEOUT);
    }

    public PQCP(final Path path, final String connInfo, final int minPoolSize, final int maxPoolSize,
                final Duration connectTimeout) throws Exception {

        if (minPoolSize > maxPoolSize) {
            throw new IllegalArgumentException("minPoolSize > maxPoolSize");
        }

        this.minPoolSize = minPoolSize > 0 ? minPoolSize : DEFAULT_MIN_POOL_SIZE;
        this.maxPoolSize = maxPoolSize > 0 ? maxPoolSize : DEFAULT_MAX_POOL_SIZE;
        this.poolSize = new AtomicInteger(minPoolSize);
        this.connectTimeout = connectTimeout != null ? connectTimeout : DEFAULT_CONNECT_TIMEOUT;
        this.notAvailableConnectionCounter = new AtomicInteger(0);

        this.pqx = new PQX(path);
        this.connInfo = connInfo;
        this.connections = new MemorySegment[this.maxPoolSize];
        this.locks = new Semaphore[this.maxPoolSize];

        if (!connect(this)) {
            throw new Exception("could not build connection pool successfully!");
        }
    }

    public MemorySegment exec(final String command) throws Throwable {
        final var availableIndex = getAvailableConnectionIndexLocked(true);
        try {
            return pqx.exec(connections[availableIndex], command);
        } finally {
            locks[availableIndex].release();
        }
    }

    public void clear(final MemorySegment res) throws Throwable {
        pqx.clear(res);
    }

    private int getAvailableConnectionIndexLocked(final boolean incrementNotAvailability) {
        try {
            for (int i = 0; i < maxPoolSize; i++) {
                if (locks[i] != null && locks[i].tryAcquire()) {
                    return i;
                }

                // Let's check next in connections ...
            }

            final var createNewConnectionThreshold = poolSize.get() * 1000;
            if (incrementNotAvailability && notAvailableConnectionCounter.incrementAndGet() > createNewConnectionThreshold) {
                notAvailableConnectionCounter.getAndSet(0);

                // If not reached end of connections array size!
                if (!poolSize.compareAndSet(maxPoolSize, maxPoolSize)) {
                    Thread.startVirtualThread(() -> makeNewConnection(poolSize.getAndIncrement()));
                }
            }

            Thread.sleep(1);
        } catch (Exception ex) {
            // Do nothing!
        }

        // Let's try for next time to find any available connection ...
        return getAvailableConnectionIndexLocked(false);
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
                    pqx.finish(connections[atIndex]);

                    // Releasing ...
                    connections[atIndex] = null;
                    final var lock = locks[atIndex];
                    locks[atIndex] = null;
                    lock.release();
                }

                logger.info(String.format("extended pool size to have %d connections to handle more queries", atIndex + 1));
            } catch (final Throwable th) {
                // Releasing ...
                // We reach this section only in case of exception at pqx.connectDB(arena.allocateUtf8String(connInfo))!
                // So connections[atIndex] will be null!
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
            if (connected.get()) { // To not continue making new connections when connected is false!
                try {
                    final var lock = new Semaphore(1);
                    lock.acquire();
                    cp.locks[i] = lock;
                    cp.connections[i] = cp.pqx.connectDB(connInfoMemorySegment);
                    if (cp.pqx.status(cp.connections[i]) != ConnStatusType.CONNECTION_OK) {
                        throw new Exception("could not connect to database server");
                    }
                } catch (final Throwable th) {
                    connected.compareAndSet(true, false);
                } finally {
                    cp.locks[i].release();
                    counter.countDown();
                }
            }
        }));

        counter.await();
        arena.close();
        if (!connected.get()) {
            cp.close();
        }

        return connected.get();
    }

    @Override
    public void close() throws Exception {
        for (int conn = 0; conn < maxPoolSize; conn++) {
            if (connections[conn] != null) {
                try {
                    locks[conn].acquire();
                    pqx.finish(connections[conn]);
                } catch (final Throwable th) {
                    // We must call finish for next connection.
                } finally {
                    locks[conn].release();
                }
            }
        }

        pqx.close();
    }
}
