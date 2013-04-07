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
        return neighbor(true);
    }

    @Override
    public LazyRecord previous() throws IOException, InterruptedException
    {
        return neighbor(false);
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
            close();
        }
    }

    // ArrayMapCursor interface

    TestArrayMapCursor(TestArrayMap map, AbstractKey startKey, MissingKeyAction missingKeyAction)
    {
        super(startKey, missingKeyAction);
        this.map = map;
        if (startKey == null) {
            this.position =
                missingKeyAction.forward()
                ? 0
                : (int) map.recordCount() - 1;
        } else {
            this.position = map.keys.binarySearch(startKey);
            if (this.position < 0) {
                this.position =
                    missingKeyAction.forward()
                    ? -this.position - 1
                    : -this.position - 2;
            }
        }
    }

    // For use by this class

    private LazyRecord neighbor(boolean forward) throws IOException, InterruptedException
    {
        LazyRecord neighbor = null;
        if (!done && position >= 0 && position < map.recordCount()) {
            neighbor = map.records.get(position);
            if (!isOpen(neighbor.key())) {
                neighbor = null;
            } else {
                position += forward ? 1 : -1;
            }
            if (neighbor == null) {
                close();
            }
        }
        return neighbor;
    }

    // Object state

    private final TestArrayMap map;
    private int position;
    private boolean done = false;
}
