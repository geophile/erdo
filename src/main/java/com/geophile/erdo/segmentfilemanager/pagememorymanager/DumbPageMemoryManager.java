package com.geophile.erdo.segmentfilemanager.pagememorymanager;

import com.geophile.erdo.Configuration;

import java.nio.ByteBuffer;

public class DumbPageMemoryManager extends PageMemoryManager
{
    // PageMemoryManager interface


    @Override
    public ByteBuffer takePageBuffer()
    {
        return ByteBuffer.wrap(new byte[pageSize]);
    }

    @Override
    public void returnPageBuffer(ByteBuffer pageBuffer)
    {
    }

    @Override
    public void reset()
    {
    }

    // DumbPageMemoryManager interface

    public DumbPageMemoryManager(Configuration configuration)
    {
        super(configuration);
    }
}
