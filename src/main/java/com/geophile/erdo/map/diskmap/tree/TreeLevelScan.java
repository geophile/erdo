/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap.tree;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.diskmap.DiskPage;
import com.geophile.erdo.util.IdGenerator;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

class TreeLevelScan extends MapScan
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("TreeLevelScan(%s)", id);
    }

    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        TreePosition next = null;
        if (!closed) {
            if (end != null && position.equals(end)) {
                next = endInclusive ? position.copy() : null;
                close();
            } else {
                next = position.copy();
                position.advanceRecord();
                if (position.atEnd()) {
                    close();
                }
            }
        }
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "{0} next: {1}", new Object[]{this, next});
        }
        return next;
    }

    @Override
    public void close()
    {
        if (!closed) {
            position.destroyRecordReference();
            // Don't call end.destroyRecordReference. end is passed in, and is not owned by this.
            closed = true;
            if (LOG.isLoggable(Level.INFO)) {
                LOG.log(Level.INFO, "{0} closed", this);
            }
        }
    }

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        // Should already be on leaf level, and we should be looking for a key known to be present (if it's within
        // the current page's range of keys).
        assert position.level().levelNumber() == 0 : position;
        DiskPage page = position.page();
        // If the key we're looking for is on the current page then find it there. Otherwise search from the root.
        if (key.compareTo(page.firstKey()) >= 0 && key.compareTo(page.lastKey()) <= 0) {
            position.recordNumber(tree.recordNumber(position.page(), TreePositionComparison.GE, key));
        } else {
            position.level(tree.levels() - 1).firstSegmentOfLevel().firstPageOfSegment();
            tree.descendToLeaf(position, TreePositionComparison.GE, key);
        }
    }

    // TreeLevelScan interface

    static TreeLevelScan endInclusive(TreePosition start, TreePosition end)
    {
        return new TreeLevelScan(start, end, true);
    }

    static TreeLevelScan endExclusive(TreePosition start, TreePosition end)
    {
        return new TreeLevelScan(start, end, false);
    }

    // For use by this class

    private TreeLevelScan(TreePosition start, TreePosition end, boolean endInclusive)
    {
        assert start != null;
        assert end == null || start.level() == end.level();
        this.tree = start.tree();
        this.end = end;
        this.endInclusive = endInclusive;
        this.position = start.copy();
        if (LOG.isLoggable(Level.INFO)) {
            LOG.log(Level.INFO, "{0} open", this);
        }
    }

    // Class state

    private static final Logger LOG = Logger.getLogger(TreeLevelScan.class.getName());
    private static final IdGenerator idGenerator = new IdGenerator(0);

    // Object state

    private final long id = idGenerator.nextId();
    private final Tree tree;
    private final TreePosition end;
    private final boolean endInclusive;
    private final TreePosition position;
    private boolean closed = false;
}

