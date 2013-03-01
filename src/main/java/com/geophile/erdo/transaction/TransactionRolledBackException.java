package com.geophile.erdo.transaction;

public class TransactionRolledBackException extends com.geophile.erdo.TransactionRolledBackException
{
    protected TransactionRolledBackException(Transaction transaction)
    {
        super(transaction.toString());
    }
}
