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

package ir.jibit.pq.enums;

/**
 * Postgresql C library PGPing enum.
 *
 * @author Alireza Pourtaghi
 */
public enum PGPing {
    /**
     * The server is running and appears to be accepting connections.
     */
    PQPING_OK,

    /**
     * The server is running but is in a state that disallows connections (startup, shutdown, or crash recovery).
     */
    PQPING_REJECT,

    /**
     * The server could not be contacted. This might indicate that the server is not running, or that there is something
     * wrong with the given connection parameters (for example, wrong port number), or that there is a network
     * connectivity problem (for example, a firewall blocking the connection request).
     */
    PQPING_NO_RESPONSE,

    /**
     * No attempt was made to contact the server, because the supplied parameters were obviously incorrect or there was
     * some client-side problem (for example, out of memory).
     */
    PQPING_NO_ATTEMPT,

    /**
     * Ping status unknown.
     */
    UNKNOWN
}
