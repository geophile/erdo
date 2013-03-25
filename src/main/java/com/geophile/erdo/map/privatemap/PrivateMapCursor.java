/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.privatemap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;
import java.util.Iterator;

class PrivateMapCursor extends MapCursor
{
    // MapCursor interface

    @Override
    public AbstractRecord next()
    {
        AbstractRecord next = null;
        if (!closed) {
            if (iterator.hasNext()) {
                next = iterator.next();
                if (!isOpen(next.key())) {
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

    // PrivateMapCursor interface

    PrivateMapCursor(PrivateMap map, AbstractKey startKey, MissingKeyAction missingKeyAction)
    {
        super(startKey, missingKeyAction);
        this.map = map;
        this.iterator =
            startKey == null
            ? map.contents.values().iterator()
            : map.contents.tailMap(startKey).values().iterator();
    }

    // State

    private final PrivateMap map;
    private Iterator<AbstractRecord> iterator;
    private boolean closed = false;
}
