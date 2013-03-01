/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap.tree;

import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.diskmap.IndexRecord;

import java.io.IOException;

class LevelOneScanToFindLevelZeroSegments extends MapScan
{
    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LevelOneMultiRecord multiRecord = null;
        if (hi != null) {
            IndexRecord lo = hi;
            IndexRecord hiPredecessor;
            do {
                levelOnePosition.advanceRecord();
                hiPredecessor = hi;
                hi = levelOnePosition.atEnd() ? null : currentLevelOneRecord();
            } while (hi != null && levelZeroSegmentNumber(lo) == levelZeroSegmentNumber(hi));
            multiRecord = new LevelOneMultiRecord(runStart, lo, levelOnePosition, hiPredecessor);
            levelOnePosition.copyTo(runStart); // Get ready for next level-0 file
        }
        return multiRecord;
    }

    @Override
    public void close()
    {
        if (!closed) {
            runStart.destroyRecordReference();
            levelOnePosition.destroyRecordReference();
            closed = true;
        }
    }

    // TreeLevelOneScanToFindLevelZeroFiles interface

    LevelOneScanToFindLevelZeroSegments(Tree tree) throws IOException, InterruptedException
    {
        this.tree = tree;
        this.levelOnePosition =
            tree.newPosition().level(1).firstSegmentOfLevel().firstPageOfSegment().firstRecordOfPage();
        this.runStart = this.levelOnePosition.copy();
        this.hi = currentLevelOneRecord();
        assert this.hi != null : tree;
    }

    // For use by this class

    private IndexRecord currentLevelOneRecord() throws IOException, InterruptedException
    {
        return (IndexRecord) levelOnePosition.materializeRecord();
    }

    private int levelZeroSegmentNumber(IndexRecord indexRecord) throws IOException, InterruptedException
    {
        return tree.segmentNumber(indexRecord.childPageAddress());
    }

    // Object state

    private final Tree tree;
    private final TreePosition levelOnePosition;
    private final TreePosition runStart; // Points to first level one IndexRecord for a level zero segment.
    private IndexRecord hi;
    private boolean closed = false;
}
