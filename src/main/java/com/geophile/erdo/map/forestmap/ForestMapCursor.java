/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.forestmap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.forest.ForestSnapshot;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;
import java.util.logging.Logger;

/*
 * A Forest has several trees. The keys of a tree are kept in memory except for a few of the
 * biggest trees. These in-memory keys are used to speed up ForestMap scans.
 *
 * A ForestMap scan can be done in two ways:
 * 1) Put all of the forest's trees into a MergeCursor.
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
 * ForestMapMatchCursor. All other scans are handled by ForestMapRangeCursor, which implements #1.
 */

public abstract class ForestMapCursor extends MapCursor
{
    // ForestMapCursor interface

    public static ForestMapCursor newScan(ForestSnapshot forestSnapshot,
                                        AbstractKey startKey,
                                        MissingKeyAction missingKeyAction)
        throws IOException, InterruptedException
    {
        assert !(startKey == null && missingKeyAction == MissingKeyAction.CLOSE);
        return
            missingKeyAction == MissingKeyAction.CLOSE
            ? new ForestMapMatchCursor(forestSnapshot, startKey)
            : new ForestMapRangeCursor(forestSnapshot, startKey, missingKeyAction);
    }

    // For use by subclasses

    protected ForestMapCursor(ForestSnapshot forestSnapshot, AbstractKey startKey, MissingKeyAction missingKeyAction)
        throws IOException, InterruptedException
    {
        super(startKey, missingKeyAction);
        this.startKey = startKey;
        this.missingKeyAction = missingKeyAction;
        this.forestSnapshot = forestSnapshot;
    }

    // Class state

    protected static final Logger LOG = Logger.getLogger(ForestMapCursor.class.getName());

    // Object state

    protected final ForestSnapshot forestSnapshot;
    protected final AbstractKey startKey;
    protected final MissingKeyAction missingKeyAction;
    protected boolean done = false;
}
