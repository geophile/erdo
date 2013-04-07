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
import java.util.NavigableMap;

class PrivateMapKeyCursor extends MapCursor
{
    // MapCursor interface

    public AbstractRecord next()
    {
        assert forward;
        return neighbor();
    }

    public AbstractRecord previous()
    {
        assert !forward;
        return neighbor();
    }

    public void close()
    {
        closed = true;
    }

    // PrivateMapKeyCursor interface

    public PrivateMapKeyCursor(NavigableMap<AbstractKey, AbstractRecord> contents,
                               AbstractKey startKey,
                               MissingKeyAction missingKeyAction)
    {
        super(startKey, missingKeyAction);

        if (missingKeyAction == MissingKeyAction.BACKWARD) {
            this.forward = false;
            if (startKey == null) {
                this.iterator = contents.descendingMap().keySet().iterator();
            } else {
                this.iterator = contents.headMap(startKey, true).descendingMap().keySet().iterator();
            }
        } else {
            this.forward = true;
            if (startKey == null) {
                this.iterator = contents.keySet().iterator();
            } else {
                this.iterator = contents.tailMap(startKey, true).keySet().iterator();
            }
        }
    }

    // FOr use by this class

    private AbstractRecord neighbor()
    {
        AbstractKey neighbor = null;
        if (!closed) {
            if (iterator.hasNext()) {
                neighbor = iterator.next();
                if (!isOpen(neighbor)) {
                    neighbor = null;
                    close();
                }
            } else {
                close();
            }
        }
        return neighbor == null ? null : new KeyOnlyRecord(neighbor);
    }

    // Object state

    private final boolean forward;
    private final Iterator<AbstractKey> iterator;
    private boolean closed = false;
}
