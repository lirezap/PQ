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
 * Postgresql C library ConnStatusType enum.
 *
 * @author Alireza Pourtaghi
 */
public enum ExecStatusType {
    /**
     * The string sent to the server was empty.
     */
    PGRES_EMPTY_QUERY,

    /**
     * Successful completion of a command returning no data.
     */
    PGRES_COMMAND_OK,

    /**
     * Successful completion of a command returning data (such as a SELECT or SHOW).
     */
    PGRES_TUPLES_OK,

    /**
     * Copy Out (from server) data transfer started.
     */
    PGRES_COPY_OUT,

    /**
     * Copy In (to server) data transfer started.
     */
    PGRES_COPY_IN,

    /**
     * The server's response was not understood.
     */
    PGRES_BAD_RESPONSE,

    /**
     * A nonfatal error (a notice or warning) occurred.
     */
    PGRES_NONFATAL_ERROR,

    /**
     * A fatal error occurred.
     */
    PGRES_FATAL_ERROR,

    /**
     * Copy In/Out (to and from server) data transfer started. This feature is currently used only for streaming
     * replication, so this status should not occur in ordinary applications.
     */
    PGRES_COPY_BOTH,

    /**
     * The PGresult contains a single result tuple from the current command. This status occurs only when single-row
     * mode has been selected for the query (see Section 34.6).
     */
    PGRES_SINGLE_TUPLE,

    /**
     * The PGresult represents a synchronization point in pipeline mode, requested by PQpipelineSync. This status occurs
     * only when pipeline mode has been selected.
     */
    PGRES_PIPELINE_SYNC,

    /**
     * The PGresult represents a pipeline that has received an error from the server. PQgetResult must be called
     * repeatedly, and each time it will return this status code until the end of the current pipeline, at which point
     * it will return PGRES_PIPELINE_SYNC and normal processing can resume.
     */
    PGRES_PIPELINE_ABORTED,

    /**
     * Execution status unknown.
     */
    UNKNOWN
}
