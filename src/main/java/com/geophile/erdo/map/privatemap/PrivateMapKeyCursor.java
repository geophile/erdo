/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.privatemap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.map.KeyOnlyRecord;
import com.geophile.erdo.map.MapCursor;

import java.util.Iterator;
import java.util.SortedMap;

class PrivateMapKeyCursor extends MapCursor
{
    // MapCursor interface

    public AbstractRecord next()
    {
        AbstractKey next = null;
        if (!closed) {
            if (iterator.hasNext()) {
                next = iterator.next();
                if (!isOpen(next)) {
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

    // PrivateMapKeyCursor interface

    public PrivateMapKeyCursor(SortedMap<AbstractKey, AbstractRecord> contents,
                               AbstractKey startKey,
                               MissingKeyAction missingKeyAction)
    {
        super(startKey, missingKeyAction);
        this.iterator =
            startKey == null
            ? contents.keySet().iterator()
            : contents.tailMap(startKey).keySet().iterator();
    }

    // Object state

    private final Iterator<AbstractKey> iterator;
    private boolean closed = false;
}
