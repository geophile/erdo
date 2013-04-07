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
        return neighbor(true);
    }

    @Override
    public LazyRecord previous() throws IOException, InterruptedException
    {
        return neighbor(false);
    }

    @Override
    public void close()
    {
        if (!done) {
            done = true;
            cursor.close();
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
        MergeCursor combinedScan = new MergeCursor(missingKeyAction.forward());
        combinedScan.addInput(new KeyToUpdatedRecordCursor(merge(forestSnapshot.smallTrees())));
        combinedScan.addInput(merge(forestSnapshot.bigTrees()));
        combinedScan.start();
        cursor = combinedScan;
    }

    // For use by this class

    private LazyRecord updateRecord(AbstractKey key) throws IOException, InterruptedException
    {
        SealedMap map = forestSnapshot.mapContainingTransaction(key.transactionTimestamp());
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Getting record of {0} from {1}", new Object[]{key, map});
        }
        assert map != null : key;
        MapCursor cursor = keyFinder(map, key);
        LazyRecord updateRecord = cursor.next();
        assert updateRecord != null : key;
        return updateRecord;
    }

    private MapCursor keyFinder(SealedMap map, AbstractKey key) throws IOException, InterruptedException
    {
        MapCursor smallMapScan = smallMapScans.get(map.mapId());
        if (smallMapScan == null) {
            smallMapScan = map.cursor(null, MissingKeyAction.CLOSE);
            smallMapScans.put(map.mapId(), smallMapScan);
        }
        smallMapScan.goTo(key);
        return smallMapScan;
    }

    private MapCursor merge(List<SealedMap> maps) throws IOException, InterruptedException
    {
        MapCursor cursor;
        int mapSize = maps.size();
        if (mapSize == 0) {
            cursor = new EmptyMapCursor();
        } else if (mapSize == 1) {
            cursor = maps.get(0).cursor(startKey, missingKeyAction);
        } else {
            MergeCursor mergeScan = new MergeCursor();
            for (SealedMap map : maps) {
                mergeScan.addInput(map.keyScan(startKey, missingKeyAction));
            }
            mergeScan.start();
            cursor = mergeScan;
        }
        return cursor;
    }

    private LazyRecord neighbor(boolean forward) throws IOException, InterruptedException
    {
        LazyRecord neighbor = null;
        if (!done) {
            neighbor = forward ? cursor.next() : cursor.previous();
            if (neighbor == null || !isOpen(neighbor.key())) {
                close();
            }
        }
        return neighbor;
    }

    // Object state

    private final MapCursor cursor;
    private final Map<Long, MapCursor> smallMapScans = new HashMap<>(); // mapId -> MapCursor

    // Inner classes

    private class KeyToUpdatedRecordCursor extends MapCursor
    {
        // MapCursor interface

        @Override
        public LazyRecord next() throws IOException, InterruptedException
        {
            return neighbor(true);
        }

        @Override
        public LazyRecord previous() throws IOException, InterruptedException
        {
            return neighbor(false);
        }

        @Override
        public void close()
        {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }

        // KeyToUpdatedRecordCursor interface

        public KeyToUpdatedRecordCursor(MapCursor cursor)
        {
            super(null, null);
            this.cursor = cursor;
        }

        // For use by this class

        private LazyRecord neighbor(boolean forward) throws IOException, InterruptedException
        {
            LazyRecord neighbor = null;
            LazyRecord record = forward ? cursor.next() : cursor.previous();
            if (record == null) {
                close();
            } else {
                AbstractKey key = record.key();
                record.destroyRecordReference();
                neighbor = updateRecord(key);
            }
            return neighbor;
        }

        // Object state

        private MapCursor cursor;
    }
}
