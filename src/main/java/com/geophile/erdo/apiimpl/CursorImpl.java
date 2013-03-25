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
        AbstractRecord record = null;
        if (mapScan != null) {
            LazyRecord next;
            boolean deleted = false;
            checkTransaction();
            do {
                next = mapScan.next();
                if (next != null) {
                    deleted = next.key().deleted();
                    if (deleted) {
                        next.destroyRecordReference();
                    }
                }
            } while (next != null && deleted);
            if (next != null) {
                record = next.materializeRecord();
                next.destroyRecordReference();
                if (!next.prefersSerialized()) {
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

    @Override
    public void close()
    {
        if (mapScan != null) {
            checkTransaction();
            transaction.unregisterScan(this);
            mapScan.close();
            mapScan = null;
            DiskPageCache.destroyRemainingTreePositions();
        }
    }

    // CursorImpl interface

    CursorImpl(TransactionManager transactionManager, MapCursor mapScan)
    {
        this.mapScan = mapScan;
        this.transactionManager = transactionManager;
        this.transaction = transactionManager.currentTransaction();
        this.transaction.registerScan(this);
    }

    // For use by this class

    private void checkTransaction()
    {
        if (transactionManager.currentTransaction() != transaction) {
            throw new UsageError("Cursor cannot be used across transaction boundaries");
        }
    }

    // Object state

    private TransactionManager transactionManager;
    private Transaction transaction;
    private MapCursor mapScan;
}
