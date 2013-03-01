package com.geophile.erdo.map.emptymap;

import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.Factory;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.SealedMap;
import com.geophile.erdo.map.SealedMapBase;
import com.geophile.erdo.transaction.TimestampSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Used to eagerly consolidate empty maps in ConsolidationElementTracker, following read transactions.
// CET owns one EmptyMap, and as empty maps (resulting from read transactions) are added, the TimestampSet is
// updated. This greatly reduces the time needed to consolidate empty maps.

public class EmptyMap extends SealedMapBase
{
    // Consolidation.Element interface

    @Override
    public boolean durable()
    {
        return false;
    }

    // OpenOrSealedMapBase interface

    @Override
    public MapScan scan(KeyRange keyRange)
    {
        return new EmptyMapScan();
    }

    @Override
    public long recordCount()
    {
        return 0;
    }

    @Override
    public long estimatedSizeBytes()
    {
        return 0;
    }

    @Override
    public void loadForConsolidation(MapScan recordScan, MapScan keyScan)
        throws UnsupportedOperationException, IOException, InterruptedException
    {
        assert false;
    }

    @Override
    public boolean keysInMemory()
    {
        return true;
    }

    @Override
    public MapScan keyScan(KeyRange keyRange)
    {
        return scan(keyRange);
    }

    @Override
    public TimestampSet timestamps()
    {
        if (timestamps != null) {
            timestampSets.add(timestamps);
        }
        timestamps = TimestampSet.consolidate(timestampSets);
        timestampSets.clear();
        return timestamps;
    }

    // EmptyMap interface

    public void addEmpty(SealedMap map)
    {
        assert map.recordCount() == 0;
        timestampSets.add(map.timestamps());
    }

    public void removeEmpty(SealedMap map)
    {
        assert map.recordCount() == 0;
        // timestamps() consolidates timestamps and timestampSets.
        timestamps = timestamps().minus(map.timestamps());
    }

    // ConsolidationElementTracker owns an EmptyMap whose timestamps are updated (via addEmpty and removeEmpty).
    // When that EmptyMap is handed out (outside of the ConsolidationSet), the recipient must have a static view
    // of the EmptyMap. (Everything else handed out is already completely unchanging.) So a copy of the EmptyMap
    // is created. The copy must have its own TimestampSet.
    public EmptyMap copy()
    {
        EmptyMap copy = new EmptyMap(factory);
        copy.timestamps = timestamps().copy(); // Not timestamps.copy(). Need to consolidate before copying.
        return copy;
    }

    public EmptyMap(Factory factory)
    {
        super(factory);
    }

    // Object state

    private final List<TimestampSet> timestampSets = new ArrayList<TimestampSet>();
}
