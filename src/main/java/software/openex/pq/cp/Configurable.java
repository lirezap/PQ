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

import java.time.Duration;

import static java.time.Duration.ofSeconds;

/**
 * Connection pool related default configuration values.
 *
 * @author Alireza Pourtaghi
 */
interface Configurable {
    int DEFAULT_MIN_POOL_SIZE = 10;
    int DEFAULT_MAX_POOL_SIZE = 25;
    Duration DEFAULT_CONNECT_TIMEOUT = ofSeconds(5);
    int DEFAULT_MAKE_NEW_CONNECTION_COEFFICIENT = 10;
    Duration DEFAULT_CHECK_CONNECTIONS_STATUS_PERIOD = ofSeconds(5);
}
