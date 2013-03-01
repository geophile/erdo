package com.geophile.erdo.map.forestmap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.forest.ForestSnapshot;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.SealedMap;
import com.geophile.erdo.map.mergescan.MergeScan;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

class ForestMapRangeScan extends ForestMapScan
{
    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (!done) {
            next = scan.next();
            if (next == null) {
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
            for (MapScan smallMapScan : smallMapScans.values()) {
                smallMapScan.close();
            }
        }
    }

    // ForestMapRangeScan interface

    ForestMapRangeScan(ForestSnapshot forestSnapshot, KeyRange keyRange)
        throws IOException, InterruptedException
    {
        super(forestSnapshot, keyRange);
        MapScan smallTreeKeyScan = merge(forestSnapshot.smallTrees());
        MapScan bigTreeRecordScan = merge(forestSnapshot.bigTrees());
        MergeScan combinedScan = new MergeScan(TimestampMerger.only());
        combinedScan.addInput(new KeyToUpdatedRecordScan(smallTreeKeyScan));
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
        MapScan scan = mapScan(map, key);
        LazyRecord updateRecord = scan.next();
        assert updateRecord != null : key;
        return updateRecord;
    }

    private MapScan mapScan(SealedMap map, AbstractKey key) throws IOException, InterruptedException
    {
        MapScan smallMapScan = smallMapScans.get(map.mapId());
        if (smallMapScan == null) {
            smallMapScan = map.scan(null);
            smallMapScans.put(map.mapId(), smallMapScan);
        }
        smallMapScan.goTo(key);
        return smallMapScan;
    }

    private MapScan merge(List<SealedMap> maps) throws IOException, InterruptedException
    {
        MapScan scan;
        if (maps.size() == 1) {
            scan = maps.get(0).scan(keyRange);
        } else {
            MergeScan mergeScan = new MergeScan(TimestampMerger.only());
            for (SealedMap map : maps) {
                mergeScan.addInput(map.keyScan(keyRange));
            }
            mergeScan.start();
            scan = mergeScan;
        }
        return scan;
    }

    // Object state

    private final MapScan scan;
    private final Map<Long, MapScan> smallMapScans = new HashMap<>(); // mapId -> MapScan

    // Inner classes

    private class KeyToUpdatedRecordScan extends MapScan
    {
        // MapScan interface

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

        // KeyToUpdatedRecordScan interface

        public KeyToUpdatedRecordScan(MapScan scan)
        {
            this.scan = scan;
        }

        // Object state

        private MapScan scan;
    }
}
