package com.geophile.erdo;

/**
 * Error in configuration or other usage.
 */

public class UsageError extends Error
{
    public UsageError(String message)
    {
        super(message);
    }

    public UsageError(Throwable cause)
    {
        super(cause);
    }
}
