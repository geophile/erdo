/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap.tree;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.UsageError;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.config.ConfigurationKeys;
import com.geophile.erdo.map.Factory;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.diskmap.DBStructure;
import com.geophile.erdo.map.diskmap.DiskPage;
import com.geophile.erdo.map.diskmap.IndexRecord;
import com.geophile.erdo.map.diskmap.Manifest;
import com.geophile.erdo.transaction.TransactionManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Tree
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("T%s", treeId);
    }

    // Tree interface

    public MapScan scan(KeyRange keyRange) throws IOException, InterruptedException
    {
        return
            level(0).leafLevelEmpty()
            ? MapScan.EMPTY
            : leafScan(keyRange);
    }

    public MapScan consolidationScan() throws IOException, InterruptedException
    {
        // If there aren't two levels, do a normal, slow scan. This consolidates small files. Also, fast merge logic
        // is dependent on the existence of level 1, to delimit level 0 files.
        return
            levels.size() <= 1
            ? scan(null)
            : new LevelOneScanToFindLevelZeroSegments(this);
    }

    public long sizeBytes()
    {
        if (sizeBytes == -1L) {
            sizeBytes = 0;
            TreeLevel leafLevel = level(0);
            int segments = leafLevel.segments();
            for (int s = 0; s < segments; s++) {
                sizeBytes += leafLevel.segment(s).pages() * pageSizeBytes;
            }
        }
        return sizeBytes;
    }

    public void destroy()
    {
        for (TreeLevel level : levels) {
            level.destroy();
        }
    }

    public TransactionManager transactionManager()
    {
        return transactionManager;
    }

    public long treeId()
    {
        return treeId;
    }

    public int levels()
    {
        return levels.size();
    }

    public TreeLevel level(int levelNumber)
    {
        return levels.get(levelNumber);
    }

    public static WriteableTree create(Factory factory,
                                       DBStructure dbStructure,
                                       long treeId)
    {
        WriteableTree tree = new WriteableTree(factory, dbStructure, treeId);
        TreeLevel rootLevel = WriteableTreeLevel.create(tree, 0);
        tree.levels.add(rootLevel);
        return tree;
    }

    public static Tree recover(Factory factory,
                               DBStructure dbStructure,
                               Manifest manifest)
        throws IOException, InterruptedException
    {
        Tree tree = new Tree(factory, dbStructure, manifest.treeId());
        tree.recover(manifest);
        return tree;
    }

    // For use by this package

    Factory factory()
    {
        return factory;
    }

    public DBStructure dbStructure()
    {
        return dbStructure;
    }

    int pageSizeBytes()
    {
        return pageSizeBytes;
    }

    long maxFileSizeBytes()
    {
        return maxFileSizeBytes;
    }

    int segmentNumber(int pageAddress)
    {
        return pageAddress >>> pageNumberBits;
    }

    int pageNumber(int pageAddress)
    {
        return pageAddress & pageNumberMask;
    }

    int pageAddress(int segmentNumber, int pageNumber)
    {
        return (segmentNumber << pageNumberBits) | pageNumber;
    }

    TreePosition newPosition()
    {
        TreePositionPool treePositionPool = factory.threadTreePositionPool();
        TreePosition treePosition = (TreePosition) treePositionPool.takeResource();
        treePosition.initialize(this);
        return treePosition;
    }

    void descendToLeaf(TreePosition position, TreePositionComparison comparison, AbstractKey key)
        throws IOException, InterruptedException
    {
        int level = position.level().levelNumber();
        position.recordNumber(recordNumber(position.page(), comparison, key));
        if (level > 0) {
            IndexRecord indexRecord = (IndexRecord) position.materializeRecord();
            position.level(level - 1).pageAddress(indexRecord.childPageAddress());
            descendToLeaf(position, comparison, key);
        }
    }

    int recordNumber(DiskPage page, TreePositionComparison comparison, AbstractKey key)
        throws IOException, InterruptedException
    {
        int recordNumber = page.recordNumber(key);
        if (recordNumber >= 0) {
            if (comparison == TreePositionComparison.GT && recordNumber < page.nRecords() - 1) {
                recordNumber++;
            } else if (comparison == TreePositionComparison.LT && recordNumber > 0) {
                recordNumber--;
            }
        } else if (page.level() == 0) {
            // recordNumber is -p-1 where p is insertion point of key. Adjust based on comparison.
            recordNumber = -recordNumber - 1;
            if (recordNumber == page.nRecords() ||
                (comparison == TreePositionComparison.LT || comparison == TreePositionComparison.LE) &&
                recordNumber > 0) {
                recordNumber--;
            }
        } else {
            // recordNumber is -p-1 where p is insertion point of key. We are above the leaf level so
            // we want the preceding record. if p = 0, then either this is the left most node (page 0),
            // or we made a mistake getting here from the parent.
            assert page.level() > 0 : key;
            if (recordNumber == -1) {
                int pageNumber = pageNumber(page.pageAddress());
                assert pageNumber == 0 : key;
                recordNumber = 0;
            } else {
                recordNumber = -recordNumber - 2;
            }
        }
        assert recordNumber >= 0 && recordNumber < page.nRecords() : key;
        return recordNumber;
    }

    // For use by this class

    private MapScan leafScan(KeyRange keyRange) throws IOException, InterruptedException
    {
        MapScan treeLevelScan;
        if (keyRange == null) {
            // TODO: This does an unnecessary page read if the usage is to create a scan and then position
            // TODO: it as necessary from ForestMapScan. Allowing null KeyRange on scan constructor is a bad idea.
            TreePosition start = newPosition().level(0).firstSegmentOfLevel().firstPageOfSegment().firstRecordOfPage();
            treeLevelScan = TreeLevelScan.endInclusive(start, null);
        } else if (keyRange.singleKey() && !level(0).keyPossiblyPresent(keyRange.lo())) {
            treeLevelScan = MapScan.EMPTY;
        } else {
            TreePosition left = newPosition().level(levels.size() - 1).firstSegmentOfLevel().firstPageOfSegment();
            descendToLeaf(left,
                          keyRange.loInclusive() ? TreePositionComparison.GE : TreePositionComparison.GT,
                          keyRange.lo());
            TreePosition right;
            if (keyRange.singleKey()) {
                right = left;
            } else {
                right = newPosition().level(levels.size() - 1).firstSegmentOfLevel().firstPageOfSegment();
                descendToLeaf(right,
                              keyRange.hiInclusive() ? TreePositionComparison.LE : TreePositionComparison.LT,
                              keyRange.hi());
            }
            treeLevelScan = !(keyRange.classify(left.materializeRecord().key()) == KeyRange.KEY_IN_RANGE &&
                              keyRange.classify(right.materializeRecord().key()) == KeyRange.KEY_IN_RANGE)
                            ? MapScan.EMPTY
                            : TreeLevelScan.endInclusive(left, right);
        }
        return treeLevelScan;
    }

    // For use by subclasses

    protected Tree(Factory factory, DBStructure dbStructure, long treeId)
    {
        this.treeId = treeId;
        this.factory = factory;
        this.transactionManager = factory.transactionManager();
        this.dbStructure = dbStructure;
        this.pageSizeBytes = factory.configuration().diskPageSizeBytes();
        this.maxFileSizeBytes = factory.configuration().diskSegmentSizeBytes();
        this.pageNumberBits = com.geophile.erdo.util.Math.ceilLog2((int) (maxFileSizeBytes / pageSizeBytes));
        this.pageNumberMask = (1 << pageNumberBits) - 1;
        if (maxFileSizeBytes % pageSizeBytes != 0) {
            throw new UsageError(String.format("%s (%s) is not divisible by %s (%s)",
                                                         ConfigurationKeys.DISK_SEGMENT_SIZE_BYTES,
                                                         maxFileSizeBytes,
                                                         ConfigurationKeys.DISK_PAGE_SIZE_BYTES,
                                                         pageSizeBytes));
        }
    }

    // For use by this class

    private void recover(Manifest manifest) throws IOException, InterruptedException
    {
        for (int level = 0; level < manifest.levels(); level++) {
            levels.add(TreeLevel.recover(this, level, manifest));
        }
    }

    // Class state

    protected static final Logger LOG = Logger.getLogger(Tree.class.getName());

    // Object state

    protected final long treeId;
    protected final Factory factory;
    protected final TransactionManager transactionManager;
    protected final DBStructure dbStructure;
    protected final int pageSizeBytes;
    protected final long maxFileSizeBytes;
    protected final int pageNumberBits;
    protected final int pageNumberMask;
    protected final List<TreeLevel> levels = new ArrayList<>();
    private long sizeBytes = -1L;
}
