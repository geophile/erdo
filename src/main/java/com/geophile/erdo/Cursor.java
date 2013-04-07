/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo;

import java.io.IOException;

/**
 * A {@link Cursor} object is used to visit the records of an {@link com.geophile.erdo.OrderedMap}.
 * Key order is defined by {@link com.geophile.erdo.AbstractKey#compareTo(AbstractKey)}.
 */

public abstract class Cursor
{
    /**
     * If this cursor is positioned on a record, then the current record is returned, and the cursor is
     * moved to the record with the next larger key. If the cursor is closed (i.e., not positioned on a record),
     * then null is returned.
     * @return The current record of the cursor, or null if the cursor is closed.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract AbstractRecord next() throws IOException, InterruptedException;

    /**
     * If this cursor is positioned on a record, then the current record is returned, and the cursor is
     * moved to the record with the next smaller key. If the cursor is closed (i.e., not positioned on a record),
     * then null is returned.
     * @return The current record of the cursor, or null if the cursor is closed.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract AbstractRecord previous() throws IOException, InterruptedException;

    /**
     * Closes the cursor. After close() returns, the cursor is not positioned on any record, and subsequent
     * calls to {@link #next()} will return null.
     */
    public abstract void close();
}
