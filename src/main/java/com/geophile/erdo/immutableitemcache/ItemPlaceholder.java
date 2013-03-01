package com.geophile.erdo.immutableitemcache;

public class ItemPlaceholder<ID, ITEM> extends CacheEntry<ID, ITEM>
{
    @Override
    public ITEM item()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean placeholder()
    {
        return true;
    }

    @Override
    public Thread owner()
    {
        return owner;
    }

    public static <ID, ITEM> ItemPlaceholder forCurrentThread()
    {
        return new ItemPlaceholder<ID, ITEM>();
    }

    private ItemPlaceholder()
    {
        this.owner = Thread.currentThread();
    }

    private final Thread owner;
}
