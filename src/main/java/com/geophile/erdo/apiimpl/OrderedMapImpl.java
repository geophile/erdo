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
    public Scan scan() throws IOException, InterruptedException
    {
        return scan(null);
    }

    @Override
    public Scan scan(Keys keys) throws IOException, InterruptedException
    {
        KeyRange keyRange = (KeyRange) keys;
        if (keyRange != null) {
            keyRange.erdoId(erdoId);
        }
        return new ScanImpl(transactionManager, transactionalMap().scan(keyRange));
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
    }

    public int erdoId()
    {
        return erdoId;
    }

    // For use by this class

    private TransactionalMap transactionalMap()
    {
        return transactionManager.currentTransaction().transactionalMap();
    }

    // Class state

    private static final IdGenerator erdoIdGenerator = new IdGenerator(1);

    // Object state

    private final TransactionManager transactionManager;
    private final int erdoId;
}
