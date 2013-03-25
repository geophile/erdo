/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.testarraymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;

public class TestArrayMapCursor extends MapCursor
{
    // MapCursor interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (!done && position < map.recordCount()) {
            next = map.records.get(position);
            if (!isOpen(next.key())) {
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

    // ArrayMapCursor interface

    TestArrayMapCursor(TestArrayMap map, AbstractKey startKey, MissingKeyAction missingKeyAction)
    {
        super(startKey, missingKeyAction);
        this.map = map;
        if (startKey == null) {
            this.position = 0;
        } else {
            this.position = map.keys.binarySearch(startKey);
            if (this.position < 0) {
                this.position = -this.position - 1;
            }
        }
    }

    // Object state

    private final TestArrayMap map;
    private int position;
    private boolean done = false;
}
