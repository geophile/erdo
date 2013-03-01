/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.arraymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;

public class ArrayMapScan extends MapScan
{
    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (!done && position < map.recordCount()) {
            next = map.records.get(position);
            if (keyRange != null && keyRange.classify(next.key()) == KeyRange.KEY_AFTER_RANGE) {
                next = null;
            } else {
                position++;
            }
            if (next == null) {
                close();
            }
        }
        return next;
    }

    @Override
    public void close()
    {
        done = true;
    }

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        position = binarySearch(key);
        if (position < 0) {
            position = -position - 1;
        }
    }

    // ArrayMapScan interface

    ArrayMapScan(ArrayMap map, KeyRange keyRange) throws IOException, InterruptedException
    {
        this.map = map;
        this.keyRange = keyRange;
        AbstractKey lo = keyRange == null ? null : keyRange.lo();
        this.position = lo == null ? 0 : binarySearch(lo);
        if (this.position < 0) {
            this.position = -this.position - 1;
        }
    }

    // For use by this class

    // Adapted from java.util.Arrays.binarySearch0
    private int binarySearch(AbstractKey key) throws IOException, InterruptedException
    {

        int low = 0;
        int high = map.records.size() - 1;
        AbstractKey midKey = null;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            midKey = map.records.get(mid).key();
            int c = midKey.compareTo(key);
            if (c < 0) {
                low = mid + 1;
            } else if (c > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    // Object state

    private final ArrayMap map;
    private final KeyRange keyRange;
    private int position;
    private boolean done = false;
}
