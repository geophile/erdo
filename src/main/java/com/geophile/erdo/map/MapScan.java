package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;

import java.io.IOException;

public abstract class MapScan
{
    public abstract LazyRecord next() throws IOException, InterruptedException;

    public abstract void close();

    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public static final MapScan EMPTY = new MapScan()
    {
        @Override
        public LazyRecord next() throws IOException, InterruptedException
        {
            return null;
        }

        @Override
        public void close()
        {
        }
    };
}
