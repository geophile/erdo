/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.forestmap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.forest.ForestSnapshot;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;
import com.geophile.erdo.map.SealedMap;
import com.geophile.erdo.map.emptymap.EmptyMapCursor;
import com.geophile.erdo.map.mergescan.MergeCursor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

class ForestMapRangeCursor extends ForestMapCursor
{
    // MapCursor interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (!done) {
            next = scan.next();
            if (next == null || !isOpen(next.key())) {
                close();
            }
        }
        return next;
    }

    @Override
    public void close()
    {
        if (!done) {
            done = true;
            scan.close();
            for (MapCursor smallMapScan : smallMapScans.values()) {
                smallMapScan.close();
            }
        }
    }

    // ForestMapRangeCursor interface

    ForestMapRangeCursor(ForestSnapshot forestSnapshot, AbstractKey startKey, MissingKeyAction missingKeyAction)
        throws IOException, InterruptedException
    {
        super(forestSnapshot, startKey, missingKeyAction);
        MapCursor smallTreeKeyScan = merge(forestSnapshot.smallTrees());
        MapCursor bigTreeRecordScan = merge(forestSnapshot.bigTrees());
        MergeCursor combinedScan = new MergeCursor(TimestampMerger.only());
        combinedScan.addInput(new KeyToUpdatedRecordCursor(smallTreeKeyScan));
        combinedScan.addInput(bigTreeRecordScan);
        combinedScan.start();
        scan = combinedScan;
    }

    // For use by this class

    private LazyRecord updateRecord(AbstractKey key) throws IOException, InterruptedException
    {
        SealedMap map = forestSnapshot.mapContainingTransaction(key.transactionTimestamp());
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Getting record of {0} from {1}", new Object[]{key, map});
        }
        assert map != null : key;
        MapCursor scan = mapScan(map, key);
        LazyRecord updateRecord = scan.next();
        assert updateRecord != null : key;
        return updateRecord;
    }

    private MapCursor mapScan(SealedMap map, AbstractKey key) throws IOException, InterruptedException
    {
        MapCursor smallMapScan = smallMapScans.get(map.mapId());
        if (smallMapScan == null) {
            smallMapScan = map.scan(null, MissingKeyAction.FORWARD);
            smallMapScans.put(map.mapId(), smallMapScan);
        }
        smallMapScan.goTo(key);
        return smallMapScan;
    }

    private MapCursor merge(List<SealedMap> maps) throws IOException, InterruptedException
    {
        MapCursor scan;
        int mapSize = maps.size();
        if (mapSize == 0) {
            scan = new EmptyMapCursor();
        } else if (mapSize == 1) {
            scan = maps.get(0).scan(startKey, missingKeyAction);
        } else {
            MergeCursor mergeScan = new MergeCursor(TimestampMerger.only());
            for (SealedMap map : maps) {
                mergeScan.addInput(map.keyScan(startKey, missingKeyAction));
            }
            mergeScan.start();
            scan = mergeScan;
        }
        return scan;
    }

    // Object state

    private final MapCursor scan;
    private final Map<Long, MapCursor> smallMapScans = new HashMap<>(); // mapId -> MapCursor

    // Inner classes

    private class KeyToUpdatedRecordCursor extends MapCursor
    {
        // MapCursor interface

        @Override
        public LazyRecord next() throws IOException, InterruptedException
        {
            LazyRecord next = null;
            LazyRecord record = scan.next();
            if (record == null) {
                close();
            } else {
                AbstractKey key = record.key();
                record.destroyRecordReference();
                next = updateRecord(key);
            }
            return next;
        }

        @Override
        public void close()
        {
            if (scan != null) {
                scan.close();
                scan = null;
            }
        }

        // KeyToUpdatedRecordCursor interface

        public KeyToUpdatedRecordCursor(MapCursor scan)
        {
            super(null, null);
            this.scan = scan;
        }

        // Object state

        private MapCursor scan;
    }
}
