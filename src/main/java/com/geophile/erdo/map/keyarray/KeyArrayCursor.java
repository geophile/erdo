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
import com.geophile.erdo.map.MapCursor;

public class KeyArrayCursor extends MapCursor
{
    // MapCursor interface

    public AbstractRecord next()
    {
        AbstractKey next = null;
        if (current < keys.size()) {
            // Why null is passed to keys.key: We could have currentKey be a field, and then reuse the
            // key. But we're returning a KeyOnlyRecord containing a key. If multiple KeyOnlyRecords
            // wrap the same AbstractKey object, that's bad. null forces allocation of a new key.
            AbstractKey currentKey = keys.key(current, null);
            if (isOpen(currentKey)) {
                next = currentKey;
                current++;
            }
        }
        return next == null ? null : new KeyOnlyRecord(next);
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
                assert missingKeyAction == MissingKeyAction.FORWARD;
                this.current = -this.current - 1;
            }
        }
    }

    // Object state

    private KeyArray keys;
    private int current;
}
