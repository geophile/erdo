/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.apiimpl;

import com.geophile.erdo.*;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.transactionalmap.TransactionalMap;
import com.geophile.erdo.transaction.TransactionManager;
import com.geophile.erdo.util.IdGenerator;

import java.io.IOException;

public class OrderedMapImpl extends OrderedMap
{
    // OrderedMap interface

    @Override
    public void ensurePresent(AbstractRecord record)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        // Copy the record so that changes to the record, made by the caller, don't change
        // the record in the database.
        record = record.copy();
        record.key().erdoId(erdoId);
        transactionalMap().put(record, false);
    }

    @Override
    public AbstractRecord put(AbstractRecord record)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        // Copy the record so that changes to the record, made by the caller, don't change
        // the record in the database.
        record = record.copy();
        record.key().erdoId(erdoId);
        LazyRecord lazyRecord = transactionalMap().put(record, true);
        return lazyRecord == null ? null : lazyRecord.materializeRecord();
    }

    @Override
    public void ensureDeleted(AbstractKey key)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        ensurePresent(new DeletedRecord(key));
    }

    @Override
    public AbstractRecord delete(AbstractKey key)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        return put(new DeletedRecord(key));
    }

    @Override
    public void lock(AbstractKey key)
        throws InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        transactionalMap().lock(key);
    }

    @Override
    public AbstractRecord find(AbstractKey key) throws IOException, InterruptedException
    {
        checkNotNull(key);
        if (key != null) {
            key.erdoId(erdoId);
        }
        return newScan(key, MissingKeyAction.STOP).next();
    }

    @Override
    public Scan find(AbstractKey startKey, MissingKeyAction missingKeyAction) throws IOException, InterruptedException
    {
        if (startKey != null) {
            startKey.erdoId(erdoId);
        }
        return newScan(startKey, missingKeyAction);
    }

    @Override
    public final Scan findAll() throws IOException, InterruptedException
    {
        return find(null, MissingKeyAction.FORWARD);
    }

    @Override
    public Scan first() throws IOException, InterruptedException
    {
        return newScan(erdoIdKey, MissingKeyAction.FORWARD);
    }

    // OrderedMapImpl interface

    public OrderedMapImpl(TransactionManager transactionManager)
    {
        this(transactionManager, (int) erdoIdGenerator.nextId());
    }

    public OrderedMapImpl(TransactionManager transactionManager, int erdoId)
    {
        assert transactionManager != null;
        this.transactionManager = transactionManager;
        this.erdoId = erdoId;
        this.erdoIdKey = new ErdoId(erdoId);
    }

    public int erdoId()
    {
        return erdoId;
    }

    // For use by this class

    private Scan newScan(AbstractKey key, MissingKeyAction missingKeyAction) throws IOException, InterruptedException
    {
        return new ScanImpl(transactionManager, transactionalMap().scan(key, missingKeyAction));
    }

    private TransactionalMap transactionalMap()
    {
        return transactionManager.currentTransaction().transactionalMap();
    }

    private void checkNotNull(AbstractKey key)
    {
        if (key == null) {
            throw new IllegalArgumentException();
        }
    }

    // Class state

    private static final IdGenerator erdoIdGenerator = new IdGenerator(1);

    // Object state

    private final TransactionManager transactionManager;
    private final int erdoId;
    private final ErdoId erdoIdKey;
}
