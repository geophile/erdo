package com.geophile.erdo.map.forestmap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.bloomfilter.BloomFilter;
import com.geophile.erdo.forest.ForestSnapshot;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.SealedMap;
import com.geophile.erdo.map.mergescan.MergeScan;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

class ForestMapMatchScan extends ForestMapScan
{
    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        MergeScan smallTreeRecordScan = null;
        MergeScan bigTreeRecordScan = null;
        LazyRecord next = null;
        List<SealedMap> smallTrees = null;
        try {
            if (!done) {
                // scan vs. keyScan:
                // - scan merges after getting records, but uses bloom filter to avoid getting
                //   record unnecessarily.
                // - keyScan uses KeyArrays, probably slower than bloom filter, but has the
                //   advantage of finding only the most recent.
                // - Big trees don't have KeyArrays.
                smallTreeRecordScan = new MergeScan(TimestampMerger.only());
                smallTrees = forestSnapshot.smallTrees();
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.log(Level.FINE, "Scanning small trees: {0}", smallTrees);
                }
                for (SealedMap smallTree : smallTrees) {
                    smallTreeRecordScan.addInput(BloomFilter.USE_BLOOM_FILTER
                                                 ? smallTree.scan(keyRange)
                                                 : smallTree.keyScan(keyRange));
                }
                smallTreeRecordScan.start();
                // If smallTreeKeyScan.next() returns a key, it is the one and only key that
                // this scan will yield. Otherwise, if the key is present, it must come from a
                // big tree.
                next = smallTreeRecordScan.next();
                if (next != null) {
                    if (!BloomFilter.USE_BLOOM_FILTER) {
                        AbstractKey key = next.key();
                        next.destroyRecordReference();
                        next = updateRecord(key);
                    }
                } else {
                    bigTreeRecordScan = new MergeScan(TimestampMerger.only());
                    for (SealedMap bigTree : forestSnapshot.bigTrees()) {
                        bigTreeRecordScan.addInput(bigTree.scan(keyRange));
                    }
                    bigTreeRecordScan.start();
                    next = bigTreeRecordScan.next();
                }
                // Because this is an exact-match scan, at most one record will be returned.
                // So whether we found one or not, we're done.
                done = true;
            }
        } finally {
            if (smallTreeRecordScan != null) {
                smallTreeRecordScan.close();
            }
            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "Scan of small trees done: {0}", smallTrees);
            }
            if (bigTreeRecordScan != null) {
                bigTreeRecordScan.close();
            }
        }
        return next;
    }

    @Override
    public void close()
    {
    }

    // ForestMapMatchScan interface

    ForestMapMatchScan(ForestSnapshot forestSnapshot, KeyRange keyRange)
        throws IOException, InterruptedException
    {
        super(forestSnapshot, keyRange);
    }

    // For use by this class

    private LazyRecord updateRecord(AbstractKey key) throws IOException, InterruptedException
    {
        SealedMap map = forestSnapshot.mapContainingTransaction(key.transactionTimestamp());
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Getting record of {0} from {1}", new Object[]{key, map});
        }
        assert map != null : key;
        MapScan scan = map.scan(new KeyRange(key, true, key, true));
        LazyRecord updateRecord = scan.next();
        scan.close();
        assert updateRecord != null : key;
        return updateRecord;
    }
}
