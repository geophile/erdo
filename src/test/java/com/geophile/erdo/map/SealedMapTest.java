/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map;

import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.TestKey;
import com.geophile.erdo.TestRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class SealedMapTest extends MapBehaviorTestBase
{
    @Test
    public void testScan() throws IOException, InterruptedException
    {
        // LOG.log(Level.SEVERE, "Re-enable all of SealedMapTest");
        for (int n = 0; n <= N_MAX; n++) {
            testScan(arrayMap(testRecords(n)), n);
            testScan(privateMap(testRecords(n)), n);
            testScan(diskMap(testRecords(n)), n);
/* Operating on a ForestMap outside the context of a TransactionalMap doesn't
   work so well due to new work on transactions & consolidation.
            testScan(forest(testRecords(n)), n);
*/
        }
    }

    private void testScan(SealedMap map, int n) throws IOException, InterruptedException
    {
//        print("testScan %s: %s", map, n);
        try {
            FACTORY.reset();
            MapScan scan;
            int expectedKey;
            int expectedLastKey;
            boolean expectedEmpty;
            LazyRecord lazyRecord;
            // Full scan
            scan = map.scan(null, MissingKeyAction.FORWARD);
            expectedKey = 0;
            while ((lazyRecord = scan.next()) != null) {
                assertEquals(expectedKey, key(lazyRecord));
                expectedKey += GAP;
//                print("scan %s", key(record));
            }
            assertEquals(n * GAP, expectedKey);
            // Try scans starting at, before, and after each key and ending at, before and after each key.
            for (int i = 0; i < n; i++) {
                int startBase = GAP * i;
                int endBase = GAP * (n - 1 - i);
                for (int start = startBase - 1; start <= startBase + 1; start++) {
                    for (int end = endBase - 1; end <= endBase + 1; end++) {
                        if (start <= end) {
                            if (!(i == 0 && start == -1 && end == 170)) {
                                continue;
                            }
//                            print("i: %s, start: %s, end: %s", i, start, end);
                            scan = map.scan(newKey(start), MissingKeyAction.FORWARD);
                            TestKey endKey = newKey(end);
                            expectedKey = start <= startBase ? startBase : startBase + GAP;
                            expectedLastKey = end >= endBase ? endBase : endBase - GAP;
                            expectedEmpty = start > end || start <= end && (end >= startBase || start <= endBase);
                            boolean empty = true;
                            while ((lazyRecord = scan.next()) != null && lazyRecord.key().compareTo(endKey) <= 0) {
//                                print("scan %s:%s: %s", start, end, key(record));
                                assertEquals(expectedKey, key(lazyRecord));
                                expectedKey += GAP;
                                empty = false;
                            }
                            if (empty) {
                                assertTrue(expectedEmpty);
                            } else {
                                assertEquals(expectedLastKey + GAP, expectedKey);
                            }
                        }
                    }
                }
            }
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

    private List<TestRecord> testRecords(int n) throws IOException
    {
        List<TestRecord> testRecords = new ArrayList<TestRecord>();
        assertTrue(GAP > 1);
        // Populate map with keys 0, GAP, ..., GAP * (n - 1)
        for (int i = 0; i < n; i++) {
            int key = GAP * i;
            TestRecord record = newRecord(key, value(key));
            testRecords.add(record);
        }
        return testRecords;
    }

    private String value(int key)
    {
        return Integer.toString(key) + FILLER;
    }

    private static final String FILLER = "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx" +
                                         "xxxxxxxxxxxxxxxxxxxx";
    private static final int N_MAX = 100;
    private static final int GAP = 10;
}
