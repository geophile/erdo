package com.geophile.erdo.segmentfilemanager.pagememorymanager;

import com.geophile.erdo.Configuration;

import java.nio.ByteBuffer;

public abstract class PageMemoryManager
{
    // PageMemoryManager interface

    public abstract ByteBuffer takePageBuffer();

    public abstract void returnPageBuffer(ByteBuffer pageBuffer);

    // For testing
    public abstract void reset();

    // For use by subclasses

    protected PageMemoryManager(Configuration configuration)
    {
        this.pageSize = configuration.diskPageSizeBytes();
        this.cacheSize = configuration.diskCacheSizeBytes();
    }

    // Object state

    protected final int pageSize;
    protected final long cacheSize;
}
