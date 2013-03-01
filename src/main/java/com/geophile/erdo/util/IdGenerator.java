package com.geophile.erdo.util;

import java.util.concurrent.atomic.AtomicLong;

public class IdGenerator
{
    public long nextId()
    {
        return idGenerator.getAndIncrement();
    }

    public void restore(long maxKnownId)
    {
        idGenerator.set(maxKnownId + 1);
    }

    public IdGenerator()
    {
        this(0);
    }

    public IdGenerator(int initialValue)
    {
        idGenerator = new AtomicLong(initialValue);
    }

    private final AtomicLong idGenerator;
}
