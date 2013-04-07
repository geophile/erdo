/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.arraymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;

public class ArrayMapCursor extends MapCursor
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
        position = binarySearch(key);
        if (position < 0) {
            // goTo is used for exact match only. So if position < 0, indicating a missing key, we're done.
            close();
        }
    }

    // ArrayMapCursor interface

    ArrayMapCursor(ArrayMap map, AbstractKey startKey, MissingKeyAction missingKeyAction)
        throws IOException, InterruptedException
    {
        super(startKey, missingKeyAction);
        this.map = map;
        if (startKey == null) {
            this.position = missingKeyAction.forward() ? 0 : (int) map.recordCount() - 1;
        } else {
            this.position = binarySearch(startKey);
            if (this.position < 0) {
                this.position = missingKeyPosition(this.position, missingKeyAction);
            }
        }
    }

    // For use by this class

    // Adapted from java.util.Arrays.binarySearch0
    private int binarySearch(AbstractKey key) throws IOException, InterruptedException
    {

        int low = 0;
        int high = map.records.size() - 1;
        AbstractKey midKey;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            midKey = map.records.get(mid).key();
            int c = midKey.compareTo(key);
            if (c < 0) {
                low = mid + 1;
            } else if (c > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low + 1);  // key not found.
    }

    private LazyRecord neighbor(boolean forward) throws IOException, InterruptedException
    {
        LazyRecord neighbor = null;
        if (!done && position >= 0 && position < map.recordCount()) {
            neighbor = map.records.get(position);
            if (!isOpen(neighbor.key())) {
                neighbor = null;
            } else {
                if (forward) {
                    position++;
                } else {
                    position--;
                }
            }
            if (neighbor == null) {
                close();
            }
        }
        return neighbor;
    }

    private static int missingKeyPosition(int position, MissingKeyAction missingKeyAction)
    {
        assert position < 0;
        return missingKeyAction.forward() ? -position - 1 : -position - 2;
    }

    // Object state

    private final ArrayMap map;
    private int position;
    private boolean done = false;
}
