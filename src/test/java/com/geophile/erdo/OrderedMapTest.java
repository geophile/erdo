/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo;

import com.geophile.erdo.apiimpl.DisklessTestDatabase;
import junit.framework.Assert;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertNull;

public class OrderedMapTest
{
    @BeforeClass
    public static void beforeClass()
    {
        FACTORY = new TestFactory();
    }

    @After
    public void after()
    {
        FACTORY.reset();
    }

    @Test
    public void testPut()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int i = 0; i < N; i++) {
            AbstractRecord replaced = map.put(TestRecord.createRecord(i, "first"));
            Assert.assertNull(replaced);
        }
        for (int i = 0; i < N; i++) {
            AbstractRecord replaced = map.put(TestRecord.createRecord(i, "second"));
            Assert.assertEquals(i, ((TestKey) replaced.key()).key());
            Assert.assertEquals("first", ((TestRecord) replaced).stringValue());
        }
        Cursor cursor = map.first();
        TestRecord record;
        int expected = 0;
        while ((record = (TestRecord) cursor.next()) != null) {
            Assert.assertEquals(expected++, record.key().key());
            Assert.assertEquals("second", record.stringValue());
        }
        Assert.assertEquals(N, expected);
        db.close();
    }

    @Test
    public void testEnsurePresent()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int i = 0; i < N; i++) {
            map.ensurePresent(TestRecord.createRecord(i, "first"));
        }
        for (int i = 0; i < N; i++) {
            map.ensurePresent(TestRecord.createRecord(i, "second"));
        }
        Cursor cursor = map.first();
        TestRecord record;
        int expected = 0;
        while ((record = (TestRecord) cursor.next()) != null) {
            Assert.assertEquals(expected++, ((TestKey) record.key()).key());
            Assert.assertEquals("second", record.stringValue());
        }
        Assert.assertEquals(N, expected);
        db.close();
    }

    @Test
    public void testDelete()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int i = 0; i < N; i++) {
            AbstractRecord replaced = map.put(TestRecord.createRecord(i, "first"));
            Assert.assertNull(replaced);
        }
        for (int i = 0; i < N; i++) {
            AbstractRecord replaced = map.delete(new TestKey(i));
            Assert.assertEquals(i, ((TestKey) replaced.key()).key());
            Assert.assertEquals("first", ((TestRecord) replaced).stringValue());
        }
        Assert.assertNull(map.first().next());
        db.close();
    }

    @Test
    public void testEnsureDeleted()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int i = 0; i < N; i++) {
            map.ensurePresent(TestRecord.createRecord(i, "first"));
        }
        for (int i = 0; i < N; i++) {
            map.ensureAbsent(new TestKey(i));
        }
        Assert.assertNull(map.first().next());
        db.close();
    }

    @Test
    public void testScan()
        throws IOException,
               InterruptedException,
               DeadlockException,
               TransactionRolledBackException
    {
        // Based on SealedMapTest.testScan
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        Cursor cursor;
        int expectedKey;
        int expectedLastKey;
        boolean expectedEmpty;
        int gap = 10;
        AbstractRecord record;
        // Load
        for (int i = 0; i < N; i++) {
            map.put(TestRecord.createRecord(i * gap, null));
        }
        // Full cursor
        cursor = map.first();
        expectedKey = 0;
        while ((record = cursor.next()) != null) {
            Assert.assertEquals(expectedKey, key(record));
            expectedKey += gap;
        }
        Assert.assertEquals(N * gap, expectedKey);
        // Try scans starting at, before, and after each key and ending at, before and after each key.
        for (int i = 0; i < N; i++) {
            int startBase = gap * i;
            int endBase = gap * (N - 1 - i);
            for (int start = startBase - 1; start <= startBase + 1; start++) {
                for (int end = endBase - 1; end <= endBase + 1; end++) {
                    if (start <= end) {
                        TestKey endKey = new TestKey(end);
                        cursor = map.cursor(new TestKey(start));
                        expectedKey = start <= startBase ? startBase : startBase + gap;
                        expectedLastKey = end >= endBase ? endBase : endBase - gap;
                        expectedEmpty = start > end || start <= end && (end >= startBase || start <= endBase);
                        boolean empty = true;
                        while ((record = cursor.next()) != null && record.key().compareTo(endKey) <= 0) {
                            Assert.assertEquals(expectedKey, key(record));
                            expectedKey += gap;
                            empty = false;
                        }
                        if (empty) {
                            Assert.assertTrue(expectedEmpty);
                        } else {
                            Assert.assertEquals(expectedLastKey + gap, expectedKey);
                        }
                    }
                }
            }
        }
        db.close();
    }

    // Problem uncovered while working on geophile-erdo. Bug #1.

    @Test
    public void testBackwardFromBeginning()
        throws IOException, InterruptedException, DeadlockException, TransactionRolledBackException
    {
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int i = 0; i < 10; i++) {
            map.ensurePresent(TestRecord.createRecord(i, null));
        }
        // map is represented by an empty forest and a PrivateMap containing updates. These will be combined
        // by a MergeCursor intent on going forward.
        Cursor cursor = map.cursor(new TestKey(-1));
        TestRecord record = (TestRecord) cursor.previous();
        assertNull(record);
    }

    @Test
    public void testForwardFromEnd()
        throws IOException, InterruptedException, DeadlockException, TransactionRolledBackException
    {
        Database db = new DisklessTestDatabase(FACTORY);
        OrderedMap map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int i = 0; i < 10; i++) {
            map.ensurePresent(TestRecord.createRecord(i, null));
        }
        // map is represented by an empty forest and a PrivateMap containing updates. These will be combined
        // by a MergeCursor intent on going forward.
        Cursor cursor = map.cursor(new TestKey(99));
        TestRecord record = (TestRecord) cursor.next();
        assertNull(record);
    }

    // End of tests for bug #1.

    private int key(AbstractRecord record)
    {
        return ((TestKey) record.key()).key();
    }

    private void dump(String label, OrderedMap map) throws IOException, InterruptedException
    {
        System.out.println(label);
        Cursor cursor = map.first();
        AbstractRecord record;
        while ((record = cursor.next()) != null) {
            System.out.println(String.format("    %s, deleted: %s", record, record.deleted()));
        }
    }

    private static TestFactory FACTORY;
    private static final String MAP_NAME = "map";
    private static final int N = 10;
}
