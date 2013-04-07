/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.transaction.TransactionUpdates;

import java.io.IOException;

public interface SealedMapOperations extends Map, TransactionUpdates
{
    /**
     * Returns the number of records contained by the map. This is not the same as cardinality,
     * since some of the contained records may have deleted() = true.
     *
     * @return Number of records contained by the map.
     */
    long recordCount();

    /**
     * Returns an estimate of the space occupied by all the keys and records in the map when
     * written to disk.
     * @return estimate of the space occupied by all the keys and records in the map when
     * written to disk.
     */
    long estimatedSizeBytes();

    /**
     * Load consolidated records.
     *
     * @param recordScan source of records to be loaded. Records are returned in key order.
     * @param keyScan    keys corresponding to the records, for use in constructing a key cache.
     *                   May be null, in which case a key cache cannot be constructed.
     *                   (Keys cannot be obtained from recordScan in general, because
     *                   consolidation does a fast merge cursor, processing multi-records
     *                   in some cases.)
     * @throws UnsupportedOperationException thrown by SealedMaps that do not support loading.
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    void loadForConsolidation(MapCursor recordScan, MapCursor keyScan)
        throws UnsupportedOperationException, IOException, InterruptedException;

    /**
     * Destroys the persistent state of a map and all records in it.
     */
    void destroyPersistentState();

    /**
     * Indicates whether this map's keys can be obtained from memory.
     *
     * @return true if this map's keys are in memory, false otherwise.
     */
    boolean keysInMemory();

    /**
     * Start a key-ordered cursor of this map's keys, starting with the given key. If the key is not
     * present, then missingKeyAction determines how to proceed:
     * - {@link MissingKeyAction#FORWARD}: Start the cursor with the smallest key present that is larger than key.
     *   If there is no such key, then the returned {@link MapCursor} is closed.
     * - {@link MissingKeyAction#BACKWARD}: Start the cursor with the largest key present that is smaller than key.
     *   If there is no such key, then the returned {@link MapCursor} is closed.
     * - {@link MissingKeyAction#CLOSE}: Return a closed {@link MapCursor}.
     * If key is null then all keys are visited. In this case, missingKeyAction must not be
     * {@link MissingKeyAction#CLOSE}.
     * @param key The starting key.
     * @param missingKeyAction Specifies where to start the cursor if key is not present.
     * @return MapCursor object representing cursor of keys, each contained in a KeyOnlyRecord.
     * @throws IOException
     * @throws InterruptedException
     */
    MapCursor keyScan(AbstractKey key, MissingKeyAction missingKeyAction) throws IOException, InterruptedException;

    /**
     * Return a cursor used to consolidate records. The records obtained from the cursor may be
     * AbstractRecords or MultiRecords. The keys from two consecutive records are disjoint and
     * in ascending order. Some implementations may not take advantage of this optimization
     * and return an ordinary cursor, which does not yield MultiRecords. (For more information,
     * see FastMergeScan.)
     *
     * @return a cursor that will visit all elements of the map in key order.
     */
    MapCursor consolidationScan() throws IOException, InterruptedException;
}
