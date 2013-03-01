package com.geophile.erdo.map;

import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.DeadlockException;
import com.geophile.erdo.TransactionRolledBackException;
import com.geophile.erdo.apiimpl.KeyRange;

import java.io.IOException;

/**
 * Base class for open maps.
 */
public abstract class OpenMapBase extends MapBase implements OpenMap
{
    // OpenMap interface

    public abstract LazyRecord put(AbstractRecord record, boolean returnReplaced)
        throws IOException, InterruptedException, DeadlockException, TransactionRolledBackException;

    public abstract MapScan scan(KeyRange keyRange)
        throws IOException, InterruptedException;

    // For use by subclasses

    protected OpenMapBase(Factory factory)
    {
        super(factory);
    }
}
