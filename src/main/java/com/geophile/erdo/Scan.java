/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo;

import java.io.IOException;

/**
 * A Scan object is used to visit the records of an {@link com.geophile.erdo.OrderedMap}. The scan order is always
 * in ascending key order as defined by {@link com.geophile.erdo.AbstractKey#compareTo(AbstractKey)}.
 */

public abstract class Scan
{
    /**
     * Return the next record of the scan, or null if there are no more records to be visited.
     * @return The next record of the scan, or null if there are no more records to be visited.
     * @throws IOException
     * @throws InterruptedException
     */
    public abstract AbstractRecord next() throws IOException, InterruptedException;

    /**
     * Terminates the scan. Subsequent calls to {@link #next()} will return null.
     */
    public abstract void close();
}
