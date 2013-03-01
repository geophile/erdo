package com.geophile.erdo.transaction;

public class DeadlockException extends com.geophile.erdo.DeadlockException
{
    protected DeadlockException(Transaction transaction)
    {
        super(transaction.toString());
    }
}
