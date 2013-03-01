package com.geophile.erdo.map.keyarray;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.KeyOnlyRecord;
import com.geophile.erdo.map.MapScan;

public class KeyArrayScan extends MapScan
{
    // MapScan interface

    public AbstractRecord next()
    {
        AbstractKey next = null;
        if (current < keys.size()) {
            // Why null is passed to keys.key: We could have currentKey be a field, and then reuse the
            // key. But we're returning a KeyOnlyRecord containing a key. If multiple KeyOnlyRecords
            // wrap the same AbstractKey object, that's bad. null forces allocation of a new key.
            AbstractKey currentKey = keys.key(current, null);
            if (keyRange == null || keyRange.classify(currentKey) == KeyRange.KEY_IN_RANGE) {
                next = currentKey;
                current++;
            }
        }
        return next == null ? null : new KeyOnlyRecord(next);
    }

    public void close()
    {
        if (keys != null) {
            current = keys.size();
            keys = null;
        }
    }

    // KeyArrayScan interface

    KeyArrayScan(KeyArray keys, KeyRange keyRange)
    {
        this.keys = keys;
        this.keyRange = keyRange;
        AbstractKey lo = keyRange == null ? null : keyRange.lo();
        if (lo == null) {
            this.current = 0;
        } else {
            this.current = keys.binarySearch(lo);
            if (this.current < 0) {
                this.current = -this.current - 1;
            }
        }
    }

    // Object state

    private final KeyRange keyRange;
    private KeyArray keys;
    private int current;
}
