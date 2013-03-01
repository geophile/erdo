/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.privatemap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.KeyOnlyRecord;
import com.geophile.erdo.map.MapScan;

import java.util.Iterator;
import java.util.SortedMap;

class PrivateMapKeyScan extends MapScan
{
    // MapScan interface

    public AbstractRecord next()
    {
        AbstractKey next = null;
        if (!closed) {
            if (iterator.hasNext()) {
                next = iterator.next();
                if (keyRange != null && keyRange.classify(next) == KeyRange.KEY_AFTER_RANGE) {
                    next = null;
                    close();
                }
            } else {
                close();
            }
        }
        return next == null ? null : new KeyOnlyRecord(next);
    }

    public void close()
    {
        closed = true;
    }

    // PrivateMapKeyScan interface

    public PrivateMapKeyScan(SortedMap<AbstractKey, AbstractRecord> contents, KeyRange keyRange)
    {
        this.keyRange = keyRange;
        AbstractKey lo = keyRange == null ? null : keyRange.lo();
        this.iterator = (lo == null ? contents : contents.tailMap(lo)).keySet().iterator();
    }

    // Object state

    private final Iterator<AbstractKey> iterator;
    private final KeyRange keyRange;
    private boolean closed = false;
}
