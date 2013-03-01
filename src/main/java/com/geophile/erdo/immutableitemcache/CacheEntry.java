package com.geophile.erdo.immutableitemcache;

public abstract class CacheEntry<ID, ITEM>
{
    public ID id()
    {
        throw new UnsupportedOperationException();
    }

    public abstract ITEM item();

    public boolean okToEvict()
    {
        throw new UnsupportedOperationException();
    }

    public int referenceCount()
    {
        throw new UnsupportedOperationException();
    }

    public boolean placeholder()
    {
        return false;
    }

    public Thread owner()
    {
        throw new UnsupportedOperationException();
    }

    public final ITEM next()
    {
        return next;
    }

    public final void next(ITEM item)
    {
        next = item;
    }

    public final boolean recentAccess()
    {
        return recentAccess;
    }

    public final void recentAccess(boolean recentAccess)
    {
        this.recentAccess = recentAccess;
    }

    private volatile ITEM next;
    private volatile boolean recentAccess;
}
