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

import ir.jibit.pq.enums.ExecStatusType;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static ir.jibit.pq.layouts.PreparedStatement.PreparedStatement_stmtName_varHandle;

/**
 * An asynchronous connection pool implementation based on {@link PQCP}.
 *
 * @author Alireza Pourtaghi
 */
public class AsyncPQCP extends PQCP {
    private final ExecutorService executor;

    public AsyncPQCP(Path path, String connInfo, ExecutorService executor) throws Exception {
        super(path, connInfo);
        this.executor = executor;
    }

    public AsyncPQCP(Path path, String connInfo, int minPoolSize, ExecutorService executor) throws Exception {
        super(path, connInfo, minPoolSize);
        this.executor = executor;
    }

    public AsyncPQCP(Path path, String connInfo, int minPoolSize, int maxPoolSize,
                     ExecutorService executor) throws Exception {

        super(path, connInfo, minPoolSize, maxPoolSize);
        this.executor = executor;
    }

    public AsyncPQCP(Path path, String connInfo, int minPoolSize, int maxPoolSize, Duration connectTimeout,
                     ExecutorService executor) throws Exception {

        super(path, connInfo, minPoolSize, maxPoolSize, connectTimeout);
        this.executor = executor;
    }

    public AsyncPQCP(Path path, String connInfo, int minPoolSize, int maxPoolSize, Duration connectTimeout,
                     int makeNewConnectionCoefficient, ExecutorService executor) throws Exception {

        super(path, connInfo, minPoolSize, maxPoolSize, connectTimeout, makeNewConnectionCoefficient);
        this.executor = executor;
    }


    public CompletableFuture<Integer> prepareThenExecuteAsync(final MemorySegment preparedStatement) {
        final var result = new CompletableFuture<Integer>();

        executor.submit(() -> {
            try {
                final var stmtName = (MemorySegment) PreparedStatement_stmtName_varHandle.get(preparedStatement);
                final var availableIndex = getAvailableConnectionIndexLocked(true, System.nanoTime(), 1);
                final var conn = connections[availableIndex];
                var connReleased = false;

                try {
                    prepareAsync(conn, preparedStatement, stmtName);
                    if (pqx.sendQueryPreparedBinaryResult(conn, preparedStatement)) {
                        final var res = loopGetResult(conn);
                        // Release as soon as possible.
                        locks[availableIndex].release();
                        connReleased = true;

                        try {
                            final var status = pqx.resultStatus(res);
                            if (status == ExecStatusType.PGRES_COMMAND_OK || status == ExecStatusType.PGRES_TUPLES_OK) {
                                result.complete(pqx.cmdTuplesInt(res));
                            } else {
                                throw new RuntimeException(String.format("status returned by database server was %s", status));
                            }
                        } finally {
                            pqx.clear(res);
                        }
                    } else {
                        throw new RuntimeException("could not submit query");
                    }
                } catch (Throwable th) {
                    result.completeExceptionally(th);
                } finally {
                    if (!connReleased) locks[availableIndex].release();
                }
            } catch (TimeoutException | RuntimeException ex) {
                result.completeExceptionally(ex);
            }
        });

        return result;
    }


    private void prepareAsync(final MemorySegment conn, final MemorySegment preparedStatement,
                              final MemorySegment stmtName) throws Throwable {

        if (pqx.sendDescribePrepared(conn, stmtName)) {
            var res = loopGetResult(conn);
            if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                // Preparing statement ...
                if (pqx.sendPrepare(conn, preparedStatement)) {
                    res = loopGetResult(conn);
                    if (pqx.resultStatus(res) != ExecStatusType.PGRES_COMMAND_OK) {
                        throw new RuntimeException(pqx.resultErrorMessageString(res));
                    }
                } else {
                    throw new RuntimeException("could not prepare statement");
                }
            }
        } else {
            throw new RuntimeException("could not check prepared statement");
        }
    }

    private MemorySegment loopGetResult(final MemorySegment conn) throws Throwable {
        for (; ; ) {
            if (pqx.consumeInput(conn)) {
                if (pqx.isBusy(conn)) {
                    Thread.sleep(1);
                } else {
                    break;
                }
            } else {
                throw new RuntimeException(pqx.errorMessage(conn).reinterpret(256).getUtf8String(0));
            }
        }

        MemorySegment call;
        MemorySegment result = null;
        for (; ; ) {
            if ((call = pqx.getResult(conn)).equals(MemorySegment.NULL)) {
                return result;
            } else {
                result = call;
            }
        }
    }
}
