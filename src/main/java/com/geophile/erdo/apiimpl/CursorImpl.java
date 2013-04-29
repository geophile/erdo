/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.apiimpl;

import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.Cursor;
import com.geophile.erdo.UsageError;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;
import com.geophile.erdo.map.diskmap.DiskPageCache;
import com.geophile.erdo.transaction.Transaction;
import com.geophile.erdo.transaction.TransactionManager;

import java.io.IOException;

public class CursorImpl extends Cursor
{
    // Cursor interface

    @Override
    public AbstractRecord next() throws IOException, InterruptedException
    {
        return neighbor(true);
    }

    @Override
    public AbstractRecord previous() throws IOException, InterruptedException
    {
        return neighbor(false);
    }

    @Override
    public void close()
    {
        if (mapCursor != null) {
            // Don't call checkTransaction. If a transaction commits and rolls back other transactions, then
            // cursors can be closed from the committing transaction's thread.
            transaction.unregisterCursor(this);
            mapCursor.close();
            mapCursor = null;
            DiskPageCache.destroyRemainingTreePositions();
        }
    }

    // CursorImpl interface

    CursorImpl(TransactionManager transactionManager, MapCursor mapCursor)
    {
        this.mapCursor = mapCursor;
        this.transactionManager = transactionManager;
        this.transaction = transactionManager.currentTransaction();
        this.transaction.registerCursor(this);
    }

    // For use by this class

    private AbstractRecord neighbor(boolean forward) throws IOException, InterruptedException
    {
        AbstractRecord record = null;
        if (mapCursor != null) {
            LazyRecord neighbor;
            boolean deleted = false;
            checkTransaction();
            do {
                neighbor = forward ? mapCursor.next() : mapCursor.previous();
                if (neighbor != null) {
                    deleted = neighbor.key().deleted();
                    if (deleted) {
                        neighbor.destroyRecordReference();
                    }
                }
            } while (neighbor != null && deleted);
            if (neighbor != null) {
                record = neighbor.materializeRecord();
                neighbor.destroyRecordReference();
                if (!neighbor.prefersSerialized()) {
                    // LazyRecord stores an actual record that is part of the database. Copy it so that any
                    // changes by the application don't modify database state.
                    record = record.copy();
                }
                // Give application a records without a timestamp set, which will allow it to update
                // the record, setting the transaction.
                record.key().clearTransactionState();
            } else {
                record = null;
                close();
            }
        }
        return record;
    }

    private void checkTransaction()
    {
        if (transactionManager.currentTransaction() != transaction) {
            throw new UsageError("Cursor cannot be used across transaction boundaries");
        }
    }

    // Object state

    private TransactionManager transactionManager;
    private Transaction transaction;
    private MapCursor mapCursor;
}
