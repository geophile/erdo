/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.mergescan;

/*
 * Like a MergeScan, but optimized for situations in which it is useful for the caller to identify
 * and handle runs of records from one input that are not interleaved with records from any other input.
 * This was inspired by an optimization of consolidation: If a leaf-level file has records that don't have
 * to be interleaved, then that file can be hard-linked into the new tree without even reading the records
 * of that file.
 * 
 * FastMergeScan handles two kinds of records, AbstractRecords as usual, and MultiRecords. A MultiRecord
 * is a sequence of AbstractRecords with ascending keys. The key of a MultiRecord is a MultiRecordKey whose
 * lo() and hi() values match the keys of the first and last members of the MultiRecord.
 * 
 * FastMergeScan inputs contain MultiRecords. The basic merge step compares the KeyRanges from two
 * MultiRecords. If they are disjoint, then the MultiRecord with the lower MultiRecordKey is promoted.
 * Otherwise, the merge goes into a "slow" mode. The member AbstractRecords from the MultiRecords are merged
 * individually. Eventually, these records are exhausted, and we go back to "fast" mode.
 * 
 * FastMergeScan.next() may return MultiRecords. The caller must check the type and handle MultiRecords appropriately.
 */

import com.geophile.erdo.map.MapScan;

public class FastMergeScan extends MergeScan
{
    // MergeScan interface

    public FastMergeScan(Merger merger)
    {
        super(merger);
    }

    // For use by this package

    Node mergeNode(int position, Node left, Node right)
    {
        return new FastMergeNode(this, position, (FastNode) left, (FastNode) right);
    }

    Node inputNode(int position, MapScan input)
    {
        return new FastInputNode(position, input);
    }

    Node fillerNode(int position)
    {
        return new FastFillerNode(position);
    }
}
