/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.MissingKeyAction;
import com.geophile.erdo.TestKey;
import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapBehaviorTestBase;
import com.geophile.erdo.map.SealedMap;
import com.geophile.erdo.map.forestmap.TimestampMerger;
import com.geophile.erdo.map.privatemap.PrivateMap;
import com.geophile.erdo.transaction.DeadlockException;
import com.geophile.erdo.transaction.TransactionRolledBackException;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class MergeScanTest extends MapBehaviorTestBase
{
    @Test
    public void testNoInputs() throws IOException, InterruptedException
    {
        MergeCursor scan = mergeScan();
        Assert.assertNull(scan.next());
    }

    @Test
    public void testOneInput()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        for (int n = 0; n <= MAX_N; n++) {
            PrivateMap map = new PrivateMap(FACTORY);
            for (int i = 0; i < n; i++) {
                map.put(newRecord(i, null), false);
            }
            MergeCursor scan = mergeScan(map);
            int expected = 0;
            AbstractRecord record;
            LazyRecord lazyRecord;
            while ((lazyRecord = scan.next()) != null) {
                record = lazyRecord.materializeRecord();
                Assert.assertEquals(expected++, key(record));
            }
            Assert.assertEquals(n, expected);
        }
    }

    @Test
    public void testTwoInputs()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        PrivateMap openMap;
        for (int nEven = 0; nEven <= MAX_N; nEven++) {
            openMap = new PrivateMap(FACTORY);
            List<Integer> expectedEven = new ArrayList<>();
            for (int i = 0; i < nEven; i++) {
                int even = 2 * i;
                openMap.put(newRecord(even, null), false);
                expectedEven.add(even);
            }
            SealedMap evenMap = openMap;
            for (int nOdd = 0; nOdd <= MAX_N; nOdd++) {
                openMap = new PrivateMap(FACTORY);
                List<Integer> expected = new ArrayList<>(expectedEven);
                // Odd numbers
                for (int i = 0; i < nEven; i++) {
                    int odd = 2 * i + 1;
                    openMap.put(newRecord(odd, null), false);
                    expected.add(odd);
                }
                Collections.sort(expected);
                Iterator<Integer> expectedIterator = expected.iterator();
                SealedMap oddMap = openMap;
                MergeCursor scan = mergeScan(evenMap, oddMap);
                LazyRecord lazyRecord;
                while ((lazyRecord = scan.next()) != null) {
                    Assert.assertEquals(expectedIterator.next().intValue(), key(lazyRecord.materializeRecord()));
                }
                Assert.assertTrue(!expectedIterator.hasNext());
            }
        }
    }

    @Test
    public void testManyInputs() throws IOException, InterruptedException
    {
        final int TRIALS = 1000;
        final int MAX_INPUTS = 20;
        final int AVE_RECORDS_PER_INPUT = 2;
        Random random = new Random(123456789);
        for (int t = 0; t < TRIALS; t++) {
            int nInputs = 1 + random.nextInt(MAX_INPUTS);
            PrivateMap[] maps = new PrivateMap[nInputs];
            for (int i = 0; i < nInputs; i++) {
                maps[i] = new PrivateMap(FACTORY);
            }
            int nRecords = nInputs * AVE_RECORDS_PER_INPUT;
            for (int k = 0; k < nRecords; k++) {
                int m = random.nextInt(nInputs);
                maps[m].put(newRecord(k, null), false);
            }
            MergeCursor scan = new MergeCursor(TimestampMerger.only());
            for (PrivateMap map : maps) {
                scan.addInput(map.scan(null, MissingKeyAction.FORWARD));
            }
            scan.start();
            LazyRecord lazyRecord;
            int expected = 0;
            while ((lazyRecord = scan.next()) != null) {
                Assert.assertEquals(expected++, ((TestKey) lazyRecord.key()).key());
            }
            Assert.assertEquals(nRecords, expected);
        }
    }

    private MergeCursor mergeScan(SealedMap... inputs) throws IOException, InterruptedException
    {
        MergeCursor mergeScan = new MergeCursor(TimestampMerger.only());
        for (SealedMap input : inputs) {
            mergeScan.addInput(input.scan(null, MissingKeyAction.FORWARD));
        }
        mergeScan.start();
        return mergeScan;
    }

    private static int MAX_N = 10;
}
