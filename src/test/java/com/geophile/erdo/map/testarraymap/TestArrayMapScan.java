/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.testarraymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;

public class TestArrayMapScan extends MapScan
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
        position = map.keys.binarySearch(key);
        if (position < 0) {
            position = -position - 1;
        }
    }

    // ArrayMapScan interface

    TestArrayMapScan(TestArrayMap map, KeyRange keyRange)
    {
        this.map = map;
        this.keyRange = keyRange;
        AbstractKey lo = keyRange == null ? null : keyRange.lo();
        this.position = lo == null ? 0 : map.keys.binarySearch(lo);
        if (this.position < 0) {
            this.position = -this.position - 1;
        }
    }

    // Object state

    private final TestArrayMap map;
    private final KeyRange keyRange;
    private int position;
    private boolean done = false;
}
