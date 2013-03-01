package com.geophile.erdo;

import java.io.IOException;

/**
 * An OrderedMap maintains a set of key/value pairs. {@link #put(AbstractRecord)} and
 * {@link #ensurePresent(AbstractRecord)} are used to associate a record with the record's key, so these
 * implement both "insert" and "update" behavior. {@link #put(AbstractRecord)}, unlike
 * {@link #ensurePresent(AbstractRecord)}, returns the record previously associated with the record's key.
 * While {@link #put(AbstractRecord)} is more generally useful, {@link #ensurePresent(AbstractRecord)} is likely
 * to be faster because the implementation does not have to find the previous record.
 *
 * <p> Similarly, {@link #delete(AbstractKey)} and {@link #ensureDeleted(AbstractKey)} both accomplish deletion.
 * {@link #delete(AbstractKey)} returns the record previously associated with the given key, while
 * {@link #ensureDeleted(AbstractKey)} does not. {@link #ensureDeleted(AbstractKey)} is therefore likely to be faster.
 *
 * <p> Retrieval of a map's records is done by using {@link #scan()} to visit all records, or {@link #scan(Keys)} to
 * visit records whose keys lie within a given range.
 *
 * <p> Keys are locked for write through {@link #put(AbstractRecord)},
 * {@link #ensurePresent(AbstractRecord)}, {@link #delete(AbstractKey)}, and {@link #ensureDeleted(AbstractKey)}.
 * To lock additional keys, call {@link #lock(AbstractKey)}.
 */

public abstract class OrderedMap
{
    /**
     * Store the record in the map, associating it with the record's key. If there was already a record
     * associated with the key, the older record is replaced. ensurePresent is usually much faster than put,
     * but does not return the replaced record.
     * @param record The record being written to the map.
     */
    public abstract void ensurePresent(AbstractRecord record)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException;

    /**
     * Store the record in the map, associating it with the record's key. If there was already a record
     * associated with the key, the older record is replaced.
     * ensurePresent is usually much faster than put, but does not return the replaced record.
     * @param record The record being written to the map.
     * @return The record previously associated with the key, or null if the key does not currently exist in the map.
     */
    public abstract AbstractRecord put(AbstractRecord record)
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException;

    /**
     * Remove from the map the record associated with the key. ensureDeleted is usually much faster than delete,
     * but does not return the deleted record.
     * @param key The key whose record is to be deleted.
     */
    public abstract void ensureDeleted(AbstractKey key)
        throws IOException, 
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException;

    /**
     * Remove from the map the record associated with the key. ensureDeleted is usually much faster than delete,
     * but does not return the deleted record.
     * @param key The key whose record is to be deleted.
     * @return The record associated with the key prior to the deletion, or null if the key does not currently
     *         exist in the map.
     */
    public abstract AbstractRecord delete(AbstractKey key)
        throws IOException, 
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException;

    /**
     * Lock the specified key for writing. This method will block if the key is already locked for
     * writing by another transaction.
     * @param key Key to be locked.
     */
    public abstract void lock(AbstractKey key)
        throws InterruptedException,
               DeadlockException,
               TransactionRolledBackException;

    /**
     * Return a scan that will visit all records present in the map in key order
     * @return a scan that will visit records whose key is between start and end inclusive.
     */
    public abstract Scan scan()
        throws IOException, 
               InterruptedException;

    /**
     * Return a scan that will visit, in key order, records whose key is in the range specified by keys.
     * If keys is null then all records are visited.
     * @param keys The range of keys to be visited.
     * @return a scan that will visit records whose key is between start and end inclusive.
     */
    public abstract Scan scan(Keys keys)
        throws IOException, 
               InterruptedException;
}
