/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;

class DiskMapCursor extends MapCursor
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
        if (treeLevelScan != null) {
            treeLevelScan.close();
            treeLevelScan = null;
        }
    }

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        treeLevelScan.goTo(key);
    }

    // DiskMapCursor interface

    DiskMapCursor(MapCursor treeLevelScan)
    {
        super(null, null);
        this.treeLevelScan = treeLevelScan;
    }

    // For use by this class

    private LazyRecord neighbor(boolean forward) throws IOException, InterruptedException
    {
        LazyRecord neighbor = null;
        if (treeLevelScan != null) {
            neighbor = forward ? treeLevelScan.next() : treeLevelScan.previous();
            if (neighbor == null) {
                close();
            }
        }
        return neighbor;
    }

    // Object state

    private MapCursor treeLevelScan;
}
