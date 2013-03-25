/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;
import java.util.Iterator;

public class IteratorScan extends MapScan
{
    // MapScan interface

    @Override
    public AbstractRecord next() throws IOException, InterruptedException
    {
        return iterator.hasNext() ? (AbstractRecord) iterator.next() : null;
    }

    @Override
    public void close()
    {
    }

    // IteratorScan interface

    public IteratorScan(Iterator iterator)
    {
        super(null, null);
        this.iterator = iterator;
    }

    // Object state

    private final Iterator iterator;
}
