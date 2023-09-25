package ir.jibit.pq.enums;

/**
 * Postgresql C library PGTransactionStatusType enum.
 *
 * @author Alireza Pourtaghi
 */
public enum PGTransactionStatusType {
    /**
     * Connection idle.
     */
    PQTRANS_IDLE,

    /**
     * Command in progress.
     */
    PQTRANS_ACTIVE,

    /**
     * Idle, within transaction block.
     */
    PQTRANS_INTRANS,

    /**
     * Idle, within failed transaction.
     */
    PQTRANS_INERROR,

    /**
     * Cannot determine status.
     */
    PQTRANS_UNKNOWN,

    UNKNOWN
}
