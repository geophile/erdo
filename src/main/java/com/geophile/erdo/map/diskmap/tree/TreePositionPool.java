package com.geophile.erdo.map.diskmap.tree;

import com.geophile.erdo.map.LazyRecord;

public class TreePositionPool extends LazyRecord.Pool
{
    @Override
    public void activate(LazyRecord lazyRecord)
    {
        ((TreePosition) lazyRecord).activate();
    }

    @Override
    public void deactivate(LazyRecord lazyRecord)
    {
        ((TreePosition) lazyRecord).deactivate();
    }

    @Override
    public TreePosition newResource()
    {
        return new TreePosition(this);
    }
}
