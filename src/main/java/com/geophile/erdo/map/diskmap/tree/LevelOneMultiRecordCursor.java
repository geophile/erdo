/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap.tree;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;
import com.geophile.erdo.util.IdGenerator;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

class LevelOneMultiRecordCursor extends MapCursor
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("LevelOneMultiRecordCursor(%s)", id);
    }

    // MapCursor interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        TreePosition next = null;
        if (state != State.DONE) {
            state = State.IN_USE;
            next = position.copy();
            if (atEnd()) {
                close();
            } else {
                position.goToNextRecord();
                if (position.atEnd()) {
                    close();
                }
            }
        }
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "{0} next: {1}", new Object[]{this, next});
        }
        return next;
    }

    @Override
    public LazyRecord previous() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        if (state != State.DONE) {
            if (position != null) {
                position.destroyRecordReference();
            }
            // Don't call end.destroyRecordReference. end is passed in, and is not owned by this.
            super.close();
            if (LOG.isLoggable(Level.INFO)) {
                LOG.log(Level.INFO, "{0} closed", this);
            }
        }
    }

    @Override
    public void goToFirst() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void goToLast() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void goTo(AbstractKey key) throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isOpen(AbstractKey key)
    {
        throw new UnsupportedOperationException();
    }

    // LevelOneMultiRecordCursor interface

    LevelOneMultiRecordCursor(TreePosition startPosition, TreePosition stopPosition)
    {
        super(null, false);
        this.position = startPosition.copy();
        this.stopPosition = stopPosition;
    }

    boolean atEnd()
    {
        return position.atEnd() || position.equals(stopPosition);
    }


    // Class state

    private static final Logger LOG = Logger.getLogger(LevelOneMultiRecordCursor.class.getName());
    private static final IdGenerator idGenerator = new IdGenerator(0);

    private final long id = idGenerator.nextId();
    private final TreePosition position;
    private final TreePosition stopPosition;
}
