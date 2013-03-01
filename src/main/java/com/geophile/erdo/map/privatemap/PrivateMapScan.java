/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.privatemap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;
import java.util.Iterator;

class PrivateMapScan extends MapScan
{
    // MapScan interface

    @Override
    public AbstractRecord next()
    {
        AbstractRecord next = null;
        if (!closed) {
            if (iterator.hasNext()) {
                next = iterator.next();
                if (next != null && 
                    keyRange != null && 
                    keyRange.classify(next.key()) == KeyRange.KEY_AFTER_RANGE) {
                    next = null;
                    close();
                }
            } else {
                close();
            }
        }
        return next;
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        iterator = map.contents.tailMap(key).values().iterator();
    }

    // PrivateMapScan interface

    PrivateMapScan(PrivateMap map, KeyRange keyRange)
    {
        this.map = map;
        this.keyRange = keyRange;
        AbstractKey lo = keyRange == null ? null : keyRange.lo();
        this.iterator = (lo == null ? map.contents : map.contents.tailMap(lo)).values().iterator();
    }

    // State

    private final PrivateMap map;
    private final KeyRange keyRange;
    private Iterator<AbstractRecord> iterator;
    private boolean closed = false;
}
