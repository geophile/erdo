/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.forest;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.mergescan.AbstractMultiRecord;

import java.io.IOException;

class RemoveDeletedRecordScan extends MapScan
{
    // MapScan interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        AbstractKey key;
        if (scan != null) {
            boolean removeDeletedRecord;
            do {
                next = scan.next();
                if (next != null &&
                    (key = next.key()).deleted() &&
                    checkSingleRecord(next) &&
                    key.transactionTimestamp() <= maxDeletionTimestamp) {
                    next.destroyRecordReference();
                    removeDeletedRecord = true;
                } else {
                    removeDeletedRecord = false;
                }
            } while (removeDeletedRecord);
            if (next == null) {
                close();
            }
        }
        return next;
    }

    @Override
    public void close()
    {
        scan = null;
    }

    // RemoveDeletedRecordScan interface

    public RemoveDeletedRecordScan(MapScan scan, long maxDeletionTimestamp)
    {
        super(null, null);
        this.scan = scan;
        this.maxDeletionTimestamp = maxDeletionTimestamp;
    }

    // For use by this class

    private boolean checkSingleRecord(LazyRecord record)
    {
        // AbstractKey.deleted(), (called in next() above), should always be false for a multi-record.
        // So if we get here, (called after next()), then the record must not be a multi-record.
        assert !(record instanceof AbstractMultiRecord) : record;
        return true;
    }

    // Object state

    private MapScan scan;
    private final long maxDeletionTimestamp;
}
