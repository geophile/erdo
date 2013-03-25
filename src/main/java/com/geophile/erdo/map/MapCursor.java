/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;

import java.io.IOException;

public abstract class MapCursor
{
    public abstract LazyRecord next() throws IOException, InterruptedException;

    public abstract void close();

    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    protected boolean isOpen(AbstractKey key)
    {
        if (key == null) {
            // scan is over because we've run off the end
            return false;
        }
        if (startKey == null) {
            // scanning the entire map, regardless of erdoId, so any non-null key is part of the scan
            return true;
        }
        if (key.erdoId() == startKey.erdoId()) {
            // In the same OrderedMap as startKey
            return !exactMatch || key.equals(startKey);
        } else {
            return false;
        }
    }

    protected MapCursor(AbstractKey startKey, MissingKeyAction missingKeyAction)
    {
        this.startKey = startKey;
        this.exactMatch = missingKeyAction == MissingKeyAction.STOP;
        this.canCheckIsOpen = !(startKey == null && missingKeyAction == null);
    }

    // Object state

    // Kinds of scans:
    // - Complete scan of map, across all erdoIds: startKey == null, exactMatch == false. Used in consolidation.
    // - Exact match: startKey != null, exactMatch = true
    // - Start at key, limited to one erdoId: startKey != null, exactMatch = false
    // - Other: canCheckIsOpen is false, meaning the actual class will check loop termination.
    private final AbstractKey startKey;
    private final boolean exactMatch;
    private final boolean canCheckIsOpen;

    // Inner classes

    public static final MapCursor EMPTY = new MapCursor(null, MissingKeyAction.FORWARD)
    {
        @Override
        public LazyRecord next() throws IOException, InterruptedException
        {
            return null;
        }

        @Override
        public void close()
        {
        }
    };
}
