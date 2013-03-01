package com.geophile.erdo.transaction;

public abstract class TransactionException extends Exception
{
    @Override
    public String getMessage()
    {
        return String.format("%s: %s", getClass().getName(), transaction);
    }

    protected TransactionException(Transaction transaction)
    {
        this.transaction = transaction;
    }
    
    private Transaction transaction;

}
