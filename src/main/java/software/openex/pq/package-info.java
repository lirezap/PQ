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

/**
 * <h2>Native postgresql access using FFM</h2>
 *
 * <p>
 * PQ is a java library that uses FFM to directly access postgresql. This library is intended to be low level, minimal
 * and with the goal of being high performance. Higher level abstractions can be built easily by using provided low
 * level functions.
 * </p>
 *
 * <p>
 * The main usable connectors are {@link software.openex.pq.PQ}, {@link software.openex.pq.PQX} (PQ extended),
 * {@link software.openex.pq.cp.PQCP} (PQ connection pool) and {@link software.openex.pq.cp.AsyncPQCP} (Asynchronous connection
 * pool). {@link software.openex.pq.PQ} is a low level FFM enabled access connector, it has methods equivalent to postgresql
 * C functions. {@link software.openex.pq.PQX} is based on {@link software.openex.pq.PQ} with some extended features.
 * {@link software.openex.pq.cp.PQCP} is a connection pool implementation based on {@link software.openex.pq.PQX} and probably
 * is the most important class of this library. {@link software.openex.pq.cp.AsyncPQCP} is the asynchronous version of
 * {@link software.openex.pq.cp.PQCP} that can be used for asynchronous database access.
 * </p>
 *
 * @author Alireza Pourtaghi
 */
package software.openex.pq;
