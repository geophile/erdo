package com.geophile.erdo.transaction;

import com.geophile.erdo.util.Interval;

import java.util.SortedMap;
import java.util.TreeMap;

// Maps an interval of timestamps to a transactional resource containing the updates from
// the transactions with those timestamps.

public class TransactionOwners
{
    public synchronized TransactionUpdates find(Long timestamp)
    {
        assert timestamp != null;
        return intervalToUpdates.get(new Interval(timestamp));
    }

    public synchronized void add(TransactionUpdates updates)
    {
        assert updates.recordCount() > 0 : updates;
        for (Interval interval : updates.timestamps()) {
            TransactionUpdates replacedMap = intervalToUpdates.put(interval, updates);
            assert replacedMap == null : replacedMap;
        }
    }

    public synchronized void remove(TransactionUpdates updates)
    {
        for (Interval interval : updates.timestamps()) {
            TransactionUpdates removedMap = intervalToUpdates.remove(interval);
            assert removedMap == updates : interval;
        }
    }

    public synchronized TransactionOwners copy()
    {
        return new TransactionOwners(new TreeMap<>(intervalToUpdates));
    }

    public TransactionOwners()
    {
        this(new TreeMap<Interval, TransactionUpdates>());
    }

    // For use by this class

    private TransactionOwners(SortedMap<Interval, TransactionUpdates> intervalToUpdates)
    {
        this.intervalToUpdates = intervalToUpdates;
    }

    // Object state

    private final SortedMap<Interval, TransactionUpdates> intervalToUpdates;
}
