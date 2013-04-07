/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.keyarray;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.map.KeyOnlyRecord;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;

public class KeyArrayCursor extends MapCursor
{
    // MapCursor interface

    public AbstractRecord next()
    {
        return neighbor(true);
    }

    @Override
    public LazyRecord previous() throws IOException, InterruptedException
    {
        return neighbor(false);
    }

    public void close()
    {
        if (keys != null) {
            current = keys.size();
            keys = null;
        }
    }

    // KeyArrayCursor interface

    KeyArrayCursor(KeyArray keys, AbstractKey startKey, MissingKeyAction missingKeyAction)
    {
        super(startKey, missingKeyAction);
        this.keys = keys;
        if (startKey == null) {
            this.current = 0;
        } else {
            this.current = keys.binarySearch(startKey);
            if (this.current < 0) {
                assert missingKeyAction.forward();
                this.current = -this.current - 1;
            }
        }
    }

    // For use by this class

    private AbstractRecord neighbor(boolean forward)
    {
        AbstractKey next = null;
        if (current >= 0 && current < keys.size()) {
            // Why null is passed to keys.key: We could have currentKey be a field, and then reuse the
            // key. But we're returning a KeyOnlyRecord containing a key. If multiple KeyOnlyRecords
            // wrap the same AbstractKey object, that's bad. null forces allocation of a new key.
            AbstractKey currentKey = keys.key(current, null);
            if (isOpen(currentKey)) {
                next = currentKey;
                if (forward) {
                    current++;
                } else {
                    current--;
                }
            }
        }
        return next == null ? null : new KeyOnlyRecord(next);
    }

    // Object state

    private KeyArray keys;
    private int current;
}
