package com.geophile.erdo.apiimpl;

import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.Scan;
import com.geophile.erdo.UsageError;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.diskmap.DiskPageCache;
import com.geophile.erdo.transaction.Transaction;
import com.geophile.erdo.transaction.TransactionManager;

import java.io.IOException;

public class ScanImpl extends Scan
{
    // Scan interface

    @Override
    public AbstractRecord next() throws IOException, InterruptedException
    {
        AbstractRecord record;
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
        return record;
    }

    @Override
    public void close()
    {
        checkTransaction();
        if (mapScan != null) {
            transaction.unregisterScan(this);
            mapScan.close();
            mapScan = null;
            transaction = null;
            DiskPageCache.destroyRemainingTreePositions();
        }
    }

    // ScanImpl interface

    ScanImpl(TransactionManager transactionManager, MapScan mapScan)
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
            throw new UsageError("Scan cannot be used across transaction boundaries");
        }
    }

    // Object state

    private TransactionManager transactionManager;
    private Transaction transaction;
    private MapScan mapScan;
}
