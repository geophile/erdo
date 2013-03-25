/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo;

import com.geophile.erdo.apiimpl.KeyRange;

/**
 * A Keys object defines a range of key values, for use in defining a set of records to retrieve using a
 * {@link com.geophile.erdo.Scan} object
 * returned by {@link com.geophile.erdo.OrderedMap#scan(Keys)}.
 * @deprecated
 */

public class Keys
{
    /**
     * Defines keys in the range [key, key].
     * @param key lo and hi bound of the range.
     * @return the range [key, key].
     */
    public static Keys eq(AbstractKey key)
    {
        return new KeyRange(key, true, key, true);
    }

    /**
     * Defines keys in the range (-infinity, key).
     * @param key hi bound of the range.
     * @return the range (-infinity, key).
     */
    public static Keys lt(AbstractKey key)
    {
        return new KeyRange(null, false, key, false);
    }

    /**
     * Defines keys in the range (-infinity, key].
     * @param key hi bound of the range.
     * @return the range (-infinity, key].
     */
    public static Keys le(AbstractKey key)
    {
        return new KeyRange(null, false, key, true);
    }

    /**
     * Defines keys in the range (key, +infinity).
     * @param key hi bound of the range.
     * @return the range (key, +infinity).
     */
    public static Keys gt(AbstractKey key)
    {
        return new KeyRange(key, false, null, false);
    }

    /**
     * Defines keys in the range [key, +infinity).
     * @param key hi bound of the range.
     * @return the range [key, +infinity).
     */
    public static Keys ge(AbstractKey key)
    {
        return new KeyRange(key, true, null, false);
    }

    /**
     * Defines keys in the range (lo, hi).
     * @param lo lo bound of the range.
     * @param hi hi bound of the range.
     * @return the range (lo, hi).
     */
    public static Keys gtlt(AbstractKey lo, AbstractKey hi)
    {
        return new KeyRange(lo, false, hi, false);
    }

    /**
     * Defines keys in the range (lo, hi].
     * @param lo lo bound of the range.
     * @param hi hi bound of the range.
     * @return the range (lo, hi].
     */
    public static Keys gtle(AbstractKey lo, AbstractKey hi)
    {
        return new KeyRange(lo, false, hi, true);
    }

    /**
     * Defines keys in the range [lo, hi).
     * @param lo lo bound of the range.
     * @param hi hi bound of the range.
     * @return the range [lo, hi).
     */
    public static Keys gelt(AbstractKey lo, AbstractKey hi)
    {
        return new KeyRange(lo, true, hi, false);
    }

    /**
     * Defines keys in the range [lo, hi].
     * @param lo lo bound of the range.
     * @param hi hi bound of the range.
     * @return the range [lo, hi].
     */
    public static Keys gele(AbstractKey lo, AbstractKey hi)
    {
        return new KeyRange(lo, true, hi, true);
    }
}
