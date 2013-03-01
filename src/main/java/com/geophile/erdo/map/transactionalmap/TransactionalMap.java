/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.transactionalmap;

import com.geophile.erdo.*;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.forest.ForestSnapshot;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.OpenMapBase;
import com.geophile.erdo.map.forestmap.ForestMapScan;
import com.geophile.erdo.map.privatemap.PrivateMap;
import com.geophile.erdo.transaction.TransactionUpdates;

import java.io.IOException;

/*
 * A TransactionalMap is an updatable map for use by a single transaction. Updates are tracked in a
 * PrivateMap which is invisible outside of this object. On commit, the PrivateMap is transferred
 * to the ForestMap. Those updates are then visible to other transactions.
 */

public class TransactionalMap extends OpenMapBase
{
    // OpenMapBase interface

    @Override
    public LazyRecord put(AbstractRecord record, boolean returnReplaced)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        lock(record.key());
        LazyRecord replaced;
        LazyRecord uncommitted = updateMap().put(record, returnReplaced);
        if (returnReplaced && uncommitted == null) {
            AbstractKey key = record.key();
            MapScan scan = ForestMapScan.newScan(forestSnapshot, new KeyRange(key, true, key, true));
            LazyRecord committed = scan.next();
            scan.close();
            replaced = committed;
        } else {
            replaced = uncommitted;
        }
        return returnReplaced ? replaced : null;
    }

    public void lock(AbstractKey key)
        throws InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        database.lock(key);
    }

    @Override
    public MapScan scan(KeyRange keyRange) throws IOException, InterruptedException
    {
        return new TransactionalMapScan(this, keyRange);
    }

    // TransactionalMap interface
    
    public TransactionUpdates takeUpdates()
    {
        PrivateMap takeUpdates = updateMap();
        updates = null;
        return takeUpdates;
    }

    public TransactionalMap(ForestSnapshot forestSnapshot)
    {
        super(forestSnapshot.database().factory());
        this.database = forestSnapshot.database();
        this.forestSnapshot = forestSnapshot;
    }

    public void cleanup()
    {
        forestSnapshot.cleanup();
    }

    // For use by this class

    private PrivateMap updateMap()
    {
        if (updates == null) {
            updates = new PrivateMap(factory);
        }
        return updates;
    }

    // Object State

    private final Database database;
    final ForestSnapshot forestSnapshot;
    PrivateMap updates;
}
