/*
 * ISC License
 *
 * Copyright (c) 2024, Alireza Pourtaghi <lirezap@protonmail.com>
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

package com.lirezap.pq.cp;

import com.lirezap.pq.layout.PreparedStatement;
import com.lirezap.pq.type.ExecStatusType;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * An asynchronous connection pool implementation based on {@link PQCP}.
 *
 * @author Alireza Pourtaghi
 */
public class AsyncPQCP extends PQCP {
    private final ExecutorService executor;

    public AsyncPQCP(
            final Path path,
            final String connInfo,
            final ExecutorService executor) throws Exception {

        this(path,
                connInfo,
                DEFAULT_MIN_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD,
                executor);
    }

    public AsyncPQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final ExecutorService executor) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD,
                executor);
    }

    public AsyncPQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final ExecutorService executor) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                maxPoolSize,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD,
                executor);
    }

    public AsyncPQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final Duration connectTimeout,
            final ExecutorService executor) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                maxPoolSize,
                connectTimeout,
                DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD,
                executor);
    }

    public AsyncPQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final Duration connectTimeout,
            final int makeNewConnectionCoefficient,
            final ExecutorService executor) throws Exception {

        this(path,
                connInfo,
                minPoolSize,
                maxPoolSize,
                connectTimeout,
                makeNewConnectionCoefficient,
                DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD,
                executor);
    }

    public AsyncPQCP(
            final Path path,
            final String connInfo,
            final int minPoolSize,
            final int maxPoolSize,
            final Duration connectTimeout,
            final int makeNewConnectionCoefficient,
            final Duration checkConnectionsStatusPeriod,
            final ExecutorService executor) throws Exception {

        super(path, connInfo, minPoolSize, maxPoolSize, connectTimeout, makeNewConnectionCoefficient, checkConnectionsStatusPeriod);
        this.executor = executor;
    }

    public CompletableFuture<Void> prepareAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        final var start = System.nanoTime();
        final var result = new CompletableFuture<Void>();
        executor.submit(() -> {
            try {
                final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
                for (int i = 0; i < getMaxPoolSize(); i++) {
                    if (getLocks()[i] != null) {
                        try {
                            getLocks()[i].acquire();
                            prepareAsync(getConnections()[i], preparedStatement, stmtName, start, queryTimeout);
                        } catch (Throwable th) {
                            result.completeExceptionally(th);
                            break;
                        } finally {
                            getLocks()[i].release();
                        }
                    }
                }

                if (!result.isCompletedExceptionally()) result.complete(null);
            } catch (RuntimeException ex) {
                result.completeExceptionally(ex);
            }
        });

        return result;
    }

    public CompletableFuture<Integer> executeAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        final var start = System.nanoTime();
        final var result = new CompletableFuture<Integer>();
        executor.submit(() -> {
            try {
                final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime(), 1);
                final var conn = getConnections()[availableIndex];
                var connReleased = false;

                try {
                    if (getPqx().sendQueryPreparedBinaryResult(conn, preparedStatement)) {
                        final var res = loopGetResult(conn, start, queryTimeout);
                        try {
                            final var status = getPqx().resultStatus(res);
                            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                                // Release as soon as possible.
                                getLocks()[availableIndex].release();
                                connReleased = true;

                                result.complete(getPqx().cmdTuplesInt(res));
                            } else {
                                result.completeExceptionally(new RuntimeException(getPqx().resultErrorMessageString(res)));
                            }
                        } finally {
                            getPqx().clear(res);
                        }
                    } else {
                        result.completeExceptionally(new RuntimeException("could not submit query"));
                    }
                } catch (Throwable th) {
                    result.completeExceptionally(th);
                } finally {
                    if (!connReleased) getLocks()[availableIndex].release();
                }
            } catch (TimeoutException ex) {
                result.completeExceptionally(ex);
            }
        });

        return result;
    }

    public CompletableFuture<Integer> prepareThenExecuteAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        final var start = System.nanoTime();
        final var result = new CompletableFuture<Integer>();
        executor.submit(() -> {
            try {
                final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
                final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime(), 1);
                final var conn = getConnections()[availableIndex];
                var connReleased = false;

                try {
                    prepareAsync(conn, preparedStatement, stmtName, start, queryTimeout);
                    if (getPqx().sendQueryPreparedBinaryResult(conn, preparedStatement)) {
                        final var res = loopGetResult(conn, start, queryTimeout);
                        try {
                            final var status = getPqx().resultStatus(res);
                            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                                // Release as soon as possible.
                                getLocks()[availableIndex].release();
                                connReleased = true;

                                result.complete(getPqx().cmdTuplesInt(res));
                            } else {
                                result.completeExceptionally(new RuntimeException(getPqx().resultErrorMessageString(res)));
                            }
                        } finally {
                            getPqx().clear(res);
                        }
                    } else {
                        result.completeExceptionally(new RuntimeException("could not submit query"));
                    }
                } catch (Throwable th) {
                    result.completeExceptionally(th);
                } finally {
                    if (!connReleased) getLocks()[availableIndex].release();
                }
            } catch (TimeoutException | RuntimeException ex) {
                result.completeExceptionally(ex);
            }
        });

        return result;
    }

    public CompletableFuture<MemorySegment> fetchTextResultAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        return fetchAsync(preparedStatement, true, queryTimeout);
    }

    public CompletableFuture<MemorySegment> prepareThenFetchTextResultAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        return prepareThenFetchAsync(preparedStatement, true, queryTimeout);
    }

    public CompletableFuture<MemorySegment> fetchBinaryResultAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        return fetchAsync(preparedStatement, false, queryTimeout);
    }

    public CompletableFuture<MemorySegment> prepareThenFetchBinaryResultAsync(
            final PreparedStatement preparedStatement,
            final Duration queryTimeout) {

        return prepareThenFetchAsync(preparedStatement, false, queryTimeout);
    }

    private CompletableFuture<MemorySegment> fetchAsync(
            final PreparedStatement preparedStatement,
            final boolean text,
            final Duration queryTimeout) {

        final var start = System.nanoTime();
        final var result = new CompletableFuture<MemorySegment>();
        executor.submit(() -> {
            try {
                final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime(), 1);
                final var conn = getConnections()[availableIndex];
                var connReleased = false;

                try {
                    final var sent =
                            text ?
                                    getPqx().sendQueryPreparedTextResult(conn, preparedStatement) :
                                    getPqx().sendQueryPreparedBinaryResult(conn, preparedStatement);

                    if (sent) {
                        final var res = loopGetResult(conn, start, queryTimeout);
                        final var status = getPqx().resultStatus(res);
                        if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                            // Release as soon as possible.
                            getLocks()[availableIndex].release();
                            connReleased = true;

                            result.complete(res);
                        } else {
                            result.completeExceptionally(new RuntimeException(getPqx().resultErrorMessageString(res)));
                        }
                    } else {
                        result.completeExceptionally(new RuntimeException("could not submit query"));
                    }
                } catch (Throwable th) {
                    result.completeExceptionally(th);
                } finally {
                    if (!connReleased) getLocks()[availableIndex].release();
                }
            } catch (TimeoutException ex) {
                result.completeExceptionally(ex);
            }
        });

        return result;
    }

    private CompletableFuture<MemorySegment> prepareThenFetchAsync(
            final PreparedStatement preparedStatement,
            final boolean text,
            final Duration queryTimeout) {

        final var start = System.nanoTime();
        final var result = new CompletableFuture<MemorySegment>();
        executor.submit(() -> {
            try {
                final var stmtName = (MemorySegment) preparedStatement.var("stmtName").get(preparedStatement.getSegment());
                final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime(), 1);
                final var conn = getConnections()[availableIndex];
                var connReleased = false;

                try {
                    prepareAsync(conn, preparedStatement, stmtName, start, queryTimeout);
                    final var sent =
                            text ?
                                    getPqx().sendQueryPreparedTextResult(conn, preparedStatement) :
                                    getPqx().sendQueryPreparedBinaryResult(conn, preparedStatement);

                    if (sent) {
                        final var res = loopGetResult(conn, start, queryTimeout);
                        final var status = getPqx().resultStatus(res);
                        if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                            // Release as soon as possible.
                            getLocks()[availableIndex].release();
                            connReleased = true;

                            result.complete(res);
                        } else {
                            result.completeExceptionally(new RuntimeException(getPqx().resultErrorMessageString(res)));
                        }
                    } else {
                        result.completeExceptionally(new RuntimeException("could not submit query"));
                    }
                } catch (Throwable th) {
                    result.completeExceptionally(th);
                } finally {
                    if (!connReleased) getLocks()[availableIndex].release();
                }
            } catch (TimeoutException | RuntimeException ex) {
                result.completeExceptionally(ex);
            }
        });

        return result;
    }

    private void prepareAsync(
            final MemorySegment conn,
            final PreparedStatement preparedStatement,
            final MemorySegment stmtName,
            final long start,
            final Duration queryTimeout) throws Throwable {

        if (getPqx().sendDescribePrepared(conn, stmtName)) {
            var res = loopGetResult(conn, start, queryTimeout);
            if (getPqx().resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                // Preparing statement ...
                if (getPqx().sendPrepare(conn, preparedStatement)) {
                    res = loopGetResult(conn, start, queryTimeout);
                    if (getPqx().resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                        throw new RuntimeException(getPqx().resultErrorMessageString(res));
                    }
                } else {
                    throw new RuntimeException("could not prepare statement");
                }
            }
        } else {
            throw new RuntimeException("could not check prepared statement");
        }
    }

    private MemorySegment loopGetResult(
            final MemorySegment conn,
            final long start,
            final Duration queryTimeout) throws Throwable {

        for (; ; ) {
            if (getPqx().consumeInput(conn)) {
                if (getPqx().isBusy(conn)) {
                    if (System.nanoTime() - start >= queryTimeout.toNanos() && cancel(conn)) {
                        throw new TimeoutException(String.format("timeout of %d ms occurred while running query", queryTimeout.toMillis()));
                    }

                    Thread.sleep(1);
                } else {
                    break;
                }
            } else {
                throw new RuntimeException(getPqx().errorMessageString(conn));
            }
        }

        MemorySegment call;
        MemorySegment result = null;
        for (; ; ) {
            if ((call = getPqx().getResult(conn)).equals(MemorySegment.NULL)) {
                return result;
            } else {
                result = call;
            }
        }
    }

    private boolean cancel(
            final MemorySegment conn) {

        try {
            final var cancelPtr = getPqx().getCancel(conn);
            getPqx().cancel(cancelPtr);
            getPqx().freeCancel(cancelPtr);
        } catch (Throwable ex) {
            return false;
        }

        return true;
    }
}
