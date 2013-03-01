package com.geophile.erdo.map.forestmap;

import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.forest.ForestSnapshot;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;
import java.util.logging.Logger;

/*
 * A Forest has several trees. The keys of a tree are kept in memory except for a few of the
 * biggest trees. These in-memory keys are used to speed up ForestMap scans.
 *
 * A ForestMap scan can be done in two ways:
 * 1) Put all of the forest's trees into a MergeScan.
 * 2) Merge the keys of the biggest trees (whose keys are not in memory) with the in-memory keys
 * from the smaller trees. This merge yields the newest version of each key. For each key provided
 * by the merge, find the associated record.
 *
 * For a complete scan, #2 might be a little faster, possibly avoiding an occasional big-tree page
 * containing nothing but obsolete records.
 *
 * For a narrow scan, #2 should be a lot better. For example, suppose we have a forest with 100
 * trees and a scan that yields 10 records. With #1 we have to probe all 100 trees, just to
 * start each tree scan. With #2 we probe at most 10 trees, because we know exactly which
 * trees have relevant records.
 *
 * A probably important special case is a scan with start = end. There are three possibilities
 * (k = start = end):
 * a) k does not exist in any tree in the forest. The in-memory keys do not contain k so we search
 *    all big tree.
 * b) k is only in one or more big trees. Again, we search just the big trees.
 * c) k is in one or more big trees and in the in-memory keys, (i.e., k has been updated recently).
 *    In this case, we search one small tree and we're done.
 *
 * So we want to implement #2, to optimize start = end scans. This case is handled by
 * ForestMapMatchScan. All other scans are handled by ForestMapRangeScan, which implements #1.
 */

public abstract class ForestMapScan extends MapScan
{
    // ForestMapScan interface

    public static ForestMapScan newScan(ForestSnapshot forestSnapshot, KeyRange keyRange)
        throws IOException, InterruptedException
    {
        return
            keyRange == null
            ? new ForestMapRangeScan(forestSnapshot, null) :
            keyRange.singleKey()
            ? new ForestMapMatchScan(forestSnapshot, keyRange)
            : new ForestMapRangeScan(forestSnapshot, keyRange);
    }

    // For use by subclasses

    protected ForestMapScan(ForestSnapshot forestSnapshot, KeyRange keyRange)
        throws IOException, InterruptedException
    {
        this.forestSnapshot = forestSnapshot;
        this.keyRange = keyRange;
    }

    // Class state

    protected static final Logger LOG = Logger.getLogger(ForestMapScan.class.getName());

    // Object state

    protected final ForestSnapshot forestSnapshot;
    protected final KeyRange keyRange;
    protected boolean done = false;
}
