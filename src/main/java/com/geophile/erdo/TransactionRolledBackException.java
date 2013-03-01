package com.geophile.erdo;

/**
 * Thrown by a transaction that is rolled back due to a concurrency conflict.
 *
 * This exception is not thrown in case of a deadlock. A deadlock victim throws
 * {@link com.geophile.erdo.DeadlockException instead}.
 */

public class TransactionRolledBackException extends Exception
{
    public TransactionRolledBackException(String message)
    {
        super(message);
    }
}
