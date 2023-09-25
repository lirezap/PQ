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

    UNKNOWN
}
