/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.arraymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
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
    public MapScan scan(AbstractKey startKey, MissingKeyAction missingKeyAction)
        throws IOException, InterruptedException
    {
        return new ArrayMapScan(this, startKey, missingKeyAction);
    }

    @Override
    public MapScan keyScan(AbstractKey startKey, MissingKeyAction missingKeyAction)
        throws IOException, InterruptedException
    {
        return scan(startKey, missingKeyAction);
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
