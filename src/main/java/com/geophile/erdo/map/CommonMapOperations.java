package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.DeadlockException;
import com.geophile.erdo.TransactionRolledBackException;
import com.geophile.erdo.apiimpl.KeyRange;

import java.io.IOException;

/**
 * Interface common to all maps, whether open or sealed.
 */
public interface CommonMapOperations extends Map
{
    /**
     * Returns an identifier of this map, guaranteed to be unique within the current JVM.
     *
     * @return unique map identifier.
     */
    long mapId();

    /**
     * Return a scan that will visit, in key order, the elements of the map selected by keys.
     * If keys is null, then all records are visited.
     *
     * @param keyRange
     * @return a scan that will visit, in key order, selected elements of the map.
     */
    MapScan scan(KeyRange keyRange) throws IOException, InterruptedException;

    /**
     * Lock the specified key for writing. This method will block if the key is already locked for
     * writing by another transaction.
     * @param key Key to be locked.
     */
    void lock(AbstractKey key)
        throws InterruptedException, DeadlockException, TransactionRolledBackException;

    // For testing

    boolean isWriteable();

    boolean isSealed();
}
