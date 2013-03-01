package com.geophile.erdo.immutableitemcache;

import java.io.IOException;

public interface ImmutableItemManager<ID, ITEM>
{
    ITEM getItemForCache(ID id) throws IOException, InterruptedException;
    void cleanupItemEvictedFromCache(ITEM item) throws IOException, InterruptedException;
}
