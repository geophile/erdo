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
        assert forward;
        return neighbor();
    }

    @Override
    public AbstractRecord previous()
    {
        assert !forward;
        return neighbor();
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
        if (missingKeyAction == MissingKeyAction.BACKWARD) {
            this.forward = false;
            if (startKey == null) {
                this.iterator = map.contents.descendingMap().values().iterator();
            } else {
                this.iterator = map.contents.headMap(startKey, true).descendingMap().values().iterator();
            }
        } else {
            this.forward = true;
            if (startKey == null) {
                this.iterator = map.contents.values().iterator();
            } else {
                this.iterator = map.contents.tailMap(startKey, true).values().iterator();
            }
        }
    }

    // For use by this class

    private AbstractRecord neighbor()
    {
        AbstractRecord neighbor = null;
        if (!closed) {
            if (iterator.hasNext()) {
                neighbor = iterator.next();
                if (!isOpen(neighbor.key())) {
                    neighbor = null;
                    close();
                }
            } else {
                close();
            }
        }
        return neighbor;
    }

    // State

    private final PrivateMap map;
    private final boolean forward;
    private Iterator<AbstractRecord> iterator;
    private boolean closed = false;
}
