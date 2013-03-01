package com.geophile.erdo;

/**
 * Indicates that write/write lock dependencies formed a cycle. The transaction that throws this exception
 * was rolled back to break the cycle.
 */

public class DeadlockException extends Exception
{
    protected DeadlockException(String message)
    {
        super(message);
    }
}
