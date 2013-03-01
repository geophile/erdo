/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap;

import com.geophile.erdo.Configuration;
import com.geophile.erdo.immutableitemcache.ImmutableItemCache;
import com.geophile.erdo.immutableitemcache.ImmutableItemManager;
import com.geophile.erdo.map.diskmap.tree.TreePosition;
import com.geophile.erdo.map.diskmap.tree.TreeSegment;
import com.geophile.erdo.util.IdentitySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DiskPageCache extends ImmutableItemCache<PageId, DiskPage>
{
    public DiskPage page(TreeSegment segment, int pageNumber, ImmutableItemManager<PageId, DiskPage> diskPageReader)
        throws IOException, InterruptedException
    {
        PageId pageId = new PageId(segment.segmentId(), pageNumber);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "read {0}", pageId);
        }
        return find(pageId, diskPageReader);
    }

    public DiskPageCache(Configuration configuration)
    {
        super(cacheSlots(configuration));
        LOG.log(Level.INFO, "cache slots: {0}", cacheSlots(configuration));
    }

    private static final AtomicInteger registerCount = new AtomicInteger(0);
    public static void registerTreePosition(TreePosition treePosition)
    {
        IdentitySet<TreePosition> threadTreePositions = TREE_POSITIONS.get();
        if (threadTreePositions == null) {
            threadTreePositions = new IdentitySet<>();
            TREE_POSITIONS.set(threadTreePositions);
        }
        TreePosition replaced = threadTreePositions.add(treePosition);
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "Register disk page reference {0} -> {1}",
                    new Object[]{treePosition, threadTreePositions.size()});
/*
            if (registerCount.getAndIncrement() < 50) {
                LOG.log(Level.FINEST, "stack", new Exception());
            }
*/
        }
        assert replaced == null : treePosition;
    }

    public static void unregisterTreePosition(TreePosition treePosition)
    {
        IdentitySet<TreePosition> threadTreePositions = TREE_POSITIONS.get();
        assert threadTreePositions != null : treePosition;
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.log(Level.FINEST, "Unregister disk page reference {0} -> {1}",
                    new Object[]{treePosition, threadTreePositions.size()});
        }
        TreePosition removed = threadTreePositions.remove(treePosition);
        assert removed == treePosition
            : String.format("treePosition: %s, removed: %s", treePosition, removed);
    }

    public static void destroyRemainingTreePositions()
    {
        IdentitySet<TreePosition> threadTreePositions = TREE_POSITIONS.get();
        if (threadTreePositions != null) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.log(Level.FINEST, "Destroying {0} remaining tree positions", threadTreePositions.size());
/*
                LOG.log(Level.FINEST, "stack", new Exception());
*/
            }
            // Need a copy because TreePosition.destroyRecordReference() removes an element from threadTreePositions.
            List<TreePosition> copy = new ArrayList<>(threadTreePositions.values());
            for (TreePosition treePosition : copy) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.log(Level.FINEST, "Release disk page reference {0}", treePosition);
                }
                treePosition.destroyRecordReference();
            }
            TREE_POSITIONS.remove();
        }
    }

    // For use by this class

    private static int cacheSlots(Configuration configuration)
    {
        long cacheSizeBytes = configuration.diskCacheSizeBytes();
        int pageSizeBytes = configuration.diskPageSizeBytes();
        return (int) (cacheSizeBytes / pageSizeBytes);
    }

    // Class state

    private static final Logger LOG = Logger.getLogger(DiskPageCache.class.getName());
    private static final ThreadLocal<IdentitySet<TreePosition>> TREE_POSITIONS = new ThreadLocal<>();
}
