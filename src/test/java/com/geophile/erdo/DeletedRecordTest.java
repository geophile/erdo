package com.geophile.erdo;

import com.geophile.erdo.apiimpl.DatabaseOnDisk;
import com.geophile.erdo.util.FileUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

// Tests handling of deleted records, through consolidations that must preserve them, and consolidations that must
// drop them.

public class DeletedRecordTest
{
    @Before
    public void before() throws IOException, InterruptedException
    {
        FileUtil.deleteDirectory(DB_DIRECTORY);
        // Turn off background consolidation
        Configuration configuration = Configuration.defaultConfiguration();
        configuration.consolidationMinMapsToConsolidate(Integer.MAX_VALUE);
        configuration.consolidationMinSizeBytes(Integer.MAX_VALUE);
        configuration.consolidationThreads(0);
        db = Database.createDatabase(DB_DIRECTORY, configuration);
        map = db.createMap("test", RecordFactory.simpleRecordFactory(TestKey.class, TestRecord.class));
    }


    @Test
    public void test() throws IOException, InterruptedException, DeadlockException, TransactionRolledBackException
    {
        final int N = 10;
        String[] expected = new String[2 * N];
        {
            // Transaction 0: Load a private map with keys 0..N-1 and delete N/2
            for (int key = 0; key < N; key++) {
                map.put(TestRecord.createRecord(key, "a"));
                expected[key] = "a";
            }
            map.delete(new TestKey(N/2));
            expected[N/2] = null;
            db.commitTransaction();
        }
        {
            // Transaction 1: Load a private map with keys N..2N-1, reinsert N/2, and delete N + N/2
            for (int key = N; key < 2 * N; key++) {
                map.put(TestRecord.createRecord(key, "b"));
                expected[key] = "b";
            }
            map.put(TestRecord.createRecord(N/2, "c"));
            expected[N/2] = "c";
            map.delete(new TestKey(N + N/2));
            expected[N + N/2] = null;
            db.commitTransaction();
        }
        {
            // Force consolidation
            ((DatabaseOnDisk)db).consolidateAll();
        }
        // Check contents
        Cursor cursor = map.first();
        TestRecord record;
        for (int key = 0; key < 2 * N; key++) {
            if (expected[key] != null) {
                record = (TestRecord) cursor.next();
                assertEquals(key, record.key().key());
                assertEquals(expected[key], record.stringValue());
            }
        }
    }

    private static final File DB_DIRECTORY = new File("/tmp/erdo");

    private Database db;
    private OrderedMap map;
}
