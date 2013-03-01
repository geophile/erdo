package com.geophile.erdo.map.emptymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;

public class EmptyMapScan extends MapScan
{
    // MapScan interface

    @Override
    public LazyRecord next()
    {
        return null;
    }

    @Override
    public void close()
    {}

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {}

    // EmptyMapScan interface

    EmptyMapScan()
    { }
}
