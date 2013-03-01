/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;

class DiskMapScan extends MapScan
{
    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (treeLevelScan != null) {
            next = treeLevelScan.next();
            if (next == null) {
                close();
            }
        }
        return next;
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

    // DiskMapScan interface

    DiskMapScan(MapScan treeLevelScan)
    {
        this.treeLevelScan = treeLevelScan;
    }

    // Object state

    private MapScan treeLevelScan;
}
