package com.geophile.erdo.map.arraymap;

import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.Factory;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.SealedMapBase;
import com.geophile.erdo.transaction.TimestampSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Used for in-memory consolidations.

public class ArrayMap extends SealedMapBase
{
    // Consolidation.Element interface

    @Override
    public boolean durable()
    {
        return false;
    }

    // OpenOrSealedMapBase interface

    @Override
    public MapScan scan(KeyRange keyRange) throws IOException, InterruptedException
    {
        return new ArrayMapScan(this, keyRange);
    }

    @Override
    public long recordCount()
    {
        return records.size();
    }

    @Override
    public long estimatedSizeBytes()
    {
        return estimatedSizeBytes;
    }

    @Override
    public void loadForConsolidation(MapScan recordScan, MapScan keyScan)
        throws UnsupportedOperationException, IOException, InterruptedException
    {
        estimatedSizeBytes = 0;
        LazyRecord record;
        while ((record = recordScan.next()) != null) {
            records.add(record);
            estimatedSizeBytes += record.estimatedSizeBytes();
        }
    }

    @Override
    public boolean keysInMemory()
    {
        return true;
    }

    @Override
    public MapScan keyScan(KeyRange keyRange) throws IOException, InterruptedException
    {
        return scan(keyRange);
    }

    // ArrayMap interface

    public ArrayMap(Factory factory, TimestampSet timestamps)
    {
        super(factory);
        this.timestamps = timestamps;
    }

    // Object state

    List<LazyRecord> records = new ArrayList<>();
    private long estimatedSizeBytes;
}
