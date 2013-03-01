package com.geophile.erdo.map;

import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.apiimpl.KeyRange;

public abstract class OpenOrSealedMapBase extends SealedMapBase implements OpenOrSealedMap
{
    // OpenOrSealedMap interface

    public abstract LazyRecord put(AbstractRecord record, boolean returnReplaced);

    public abstract MapScan scan(KeyRange keyRange);

    public abstract long recordCount();

    // For use by subclasses

    protected OpenOrSealedMapBase(Factory factory)
    {
        super(factory);
    }
}
