/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.map.MapScan;

import java.io.IOException;

class FastInputNode extends FastNode
{
    public String toString()
    {
        return String.format("FastInputNode(#%s: %s)", position, key);
    }

    public void prime() throws IOException, InterruptedException
    {
        promote();
    }

    public void fastPromote() throws IOException, InterruptedException
    {
        record = input.next();
        key = record == null ? null : record.key();
    }

    public FastInputNode(int position, MapScan input)
    {
        super(position);
        this.input = input;
    }

    private final MapScan input;
}
