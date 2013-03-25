/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.emptymap;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;

public class EmptyMapCursor extends MapCursor
{
    // MapCursor interface

    @Override
    public LazyRecord next()
    {
        return null;
    }

    @Override
    public void close()
    {}

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {}

    // EmptyMapCursor interface

    public EmptyMapCursor()
    {
        super(null, MissingKeyAction.FORWARD);
    }
}
