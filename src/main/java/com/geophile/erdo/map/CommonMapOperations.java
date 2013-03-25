/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.DeadlockException;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.TransactionRolledBackException;

import java.io.IOException;

/**
 * Interface common to all maps, whether open or sealed.
 */
public interface CommonMapOperations extends Map
{
    /**
     * Returns an identifier of this map, guaranteed to be unique within the current JVM.
     *
     * @return unique map identifier.
     */
    long mapId();

    /**
     * Return a scan that will visit, in key order, the elements of the map starting with key.
     * If key is null, then records are scanned from the beginning of the map. If the key is not
     * present, then missingKeyAction determines how to proceed:
     * - {@link MissingKeyAction#FORWARD}: Start the scan with the smallest key present that is larger than key.
     *   If there is no such key, then the returned {@link MapCursor} is closed.
     * - {@link MissingKeyAction#BACKWARD}: Start the scan with the largest key present that is smaller than key.
     *   If there is no such key, then the returned {@link MapCursor} is closed.
     * - {@link MissingKeyAction#CLOSE}: Return a closed {@link MapCursor}.
     * @param key The starting key.
     * @param missingKeyAction Specifies where to start the scan if key is not present.
     * @return A {@link MapCursor} that will visit qualifying records in key order.
     * @throws IOException
     * @throws InterruptedException
     */
    MapCursor scan(AbstractKey key, MissingKeyAction missingKeyAction) throws IOException, InterruptedException;

    /**
     * Lock the specified key for writing. This method will block if the key is already locked for
     * writing by another transaction.
     * @param key Key to be locked.
     */
    void lock(AbstractKey key)
        throws InterruptedException, DeadlockException, TransactionRolledBackException;

    // For testing

    boolean isWriteable();

    boolean isSealed();
}
