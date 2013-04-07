/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo;

/**
 * Used in conjunction with {@link OrderedMap#find(AbstractKey, MissingKeyAction)} to specify where the resulting
 * {@link Cursor} starts, when the search key is not present in the map.
 */

public enum MissingKeyAction
{
    /**
     * Find the smallest key larger than the given key. If there is no such key, then the resulting {@link Cursor}
     * will be closed.
     */
    FORWARD,

    /**
     * Find the largest key smaller than the given key. If there is no such key, then the resulting {@link Cursor}
     * will be closed.
     */
    BACKWARD,

    /**
     * The resulting {@link Cursor} will be closed.
     */
    CLOSE;

    public boolean forward()
    {
        // Handle CLOSE the same as FORWARD
        return this != BACKWARD;
    }
}
