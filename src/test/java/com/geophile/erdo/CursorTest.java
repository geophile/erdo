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

import static org.junit.Assert.*;

public class CursorTest
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
    public void testFindAndNext()
        throws InterruptedException, DeadlockException, TransactionRolledBackException, IOException
    {
        for (int n = 0; n < 100; n++) {
            loadDatabase(n);
            Cursor cursor;
            TestRecord record;
            TestKey key;
            int expected;
            // Complete cursor
            {
                expected = 0;
                cursor = map.first();
                while ((record = (TestRecord) cursor.next()) != null) {
                    checkRecord(expected++, record);
                }
                assertEquals(n, expected);
/*
                expected = 0;
                cursor = map.last();
                while ((record = (TestRecord) cursor.previous()) != null) {
                    checkRecord(expected++, record);
                }
                assertEquals(n, expected);
*/
            }
            // Random access
            {
                // Test:
                // -    k * GAP - GAP/2 (missing)
                // -    k * GAP (present)
                for (int k = 0; k <= n; k++) {
                    // Test missing
                    {
                        int missingKey = k * GAP - GAP / 2;
                        key = new TestKey(missingKey);
                        // test find -> record
                        record = (TestRecord) map.find(key);
                        assertNull(record);
                        // test find -> cursor
                        cursor = map.find(key, MissingKeyAction.FORWARD);
                        expected = k;
                        while ((record = (TestRecord) cursor.next()) != null) {
                            checkRecord(expected++, record);
                        }
                    }
                    // Test present
                    if (k < n) {
                        int presentKey = k * GAP;
                        expected = k;
                        key = new TestKey(presentKey);
                        // test find -> record
                        record = (TestRecord) map.find(key);
                        checkRecord(expected, record);
                        // test find -> cursor
                        cursor = map.find(key, MissingKeyAction.FORWARD);
                        while ((record = (TestRecord) cursor.next()) != null) {
                            checkRecord(expected++, record);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testClose() throws InterruptedException, DeadlockException, TransactionRolledBackException, IOException
    {
        // Test close of cursor over empty (which starts out closed)
        {
            loadDatabase(0);
            Cursor cursor = map.first();
            assertNull(cursor.next());
            cursor.close();
            assertNull(cursor.next());
            cursor.close();
            assertNull(cursor.next());
        }
        // Test repeated close of cursor that wasn't closed to start
        {
            loadDatabase(10);
            Cursor cursor = map.first();
            assertNotNull(cursor.next());
            assertNotNull(cursor.next());
            cursor.close();
            assertNull(cursor.next());
            cursor.close();
            assertNull(cursor.next());
        }
    }

    private void loadDatabase(int n)
        throws IOException, InterruptedException, DeadlockException, TransactionRolledBackException
    {
        // map is loaded with (x * GAP, "r.x"), 0 <= x < n
        db = new DisklessTestDatabase(FACTORY);
        map = db.createMap(MAP_NAME, TestKey.class, TestRecord.class);
        for (int key = 0; key < n; key++) {
            AbstractRecord replaced = map.put(TestRecord.createRecord(testKey(key), testValue(key)));
            Assert.assertNull(replaced);
        }
    }

    private void checkRecord(int expected, TestRecord record)
    {
        assertNotNull(record);
        assertEquals(testKey(expected), record.key().key());
        assertEquals(testValue(expected), record.stringValue());
    }

    private int testKey(int x)
    {
        return x * GAP;
    }

    private String testValue(int x)
    {
        return String.format("r.%s", x);
    }

    private static TestFactory FACTORY;
    private static final String MAP_NAME = "map";
    private static final int GAP = 10;

    private Database db;
    private OrderedMap map;
}
