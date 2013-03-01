/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.transactionalmap;

import com.geophile.erdo.apiimpl.KeyRange;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;
import com.geophile.erdo.map.forestmap.ForestMapScan;
import com.geophile.erdo.map.forestmap.TimestampMerger;
import com.geophile.erdo.map.mergescan.MergeScan;

import java.io.IOException;

class TransactionalMapScan extends MapScan
{
    // Scan interface

    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (scan != null) {
            next = scan.next();
            if (next == null) {
                close();
            }
        }
        return next;
    }

    public void close()
    {
        if (scan != null) {
            scan.close();
            scan = null;
        }
    }

    // TransactionalMapScan interface

    TransactionalMapScan(TransactionalMap transactionalMap, KeyRange keyRange)
         throws IOException, InterruptedException
    {
        MapScan snapshotScan = ForestMapScan.newScan(transactionalMap.forestSnapshot, keyRange);
        if (transactionalMap.updates == null || // dynamic map was rolled back.
            transactionalMap.updates.recordCount() == 0) {
            scan = snapshotScan;
        } else {
            MergeScan mergeScan = new MergeScan(TimestampMerger.only());
            mergeScan.addInput(snapshotScan);
            mergeScan.addInput(transactionalMap.updates.scan(keyRange));
            mergeScan.start();
            scan = mergeScan;
        }
    }

    // State

    private MapScan scan;
}
