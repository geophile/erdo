/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;

import java.io.IOException;

public abstract class MapScan
{
    public abstract LazyRecord next() throws IOException, InterruptedException;

    public abstract void close();

    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public static final MapScan EMPTY = new MapScan()
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
