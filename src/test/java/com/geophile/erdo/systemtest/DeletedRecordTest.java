package com.geophile.erdo.systemtest;

import com.geophile.erdo.*;
import com.geophile.erdo.apiimpl.DatabaseOnDisk;
import com.geophile.erdo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

// Tests handling of deleted records, through consolidations that must preserve them, and consolidations that must
// drop them.

public class DeletedRecordTest
{
    @Test
    public void test() throws IOException, InterruptedException, DeadlockException, TransactionRolledBackException
    {
        FileUtil.deleteDirectory(DB_DIRECTORY);
        // Turn off background consolidation
        Configuration configuration = Configuration.defaultConfiguration();
        configuration.consolidationMinMapsToConsolidate(Integer.MAX_VALUE);
        configuration.consolidationMinSizeBytes(Integer.MAX_VALUE);
        configuration.consolidationThreads(0);
        Database db = Database.createDatabase(DB_DIRECTORY, configuration);
        OrderedMap map = db.createMap("test", RecordFactory.simpleRecordFactory(TestKey.class, TestRecord.class));
        final int N = 10;
        {
            // Transaction 0: Load a private map with keys 0..N-1 and delete N/2
            for (int key = 0; key < N; key++) {
                map.put(TestRecord.createRecord(key, "a"));
            }
            map.delete(new TestKey(N/2));
            db.commitTransaction();
        }
        {
            // Transaction 1: Load a private map with keys N..2N-1, reinsert N/2, and delete N + N/2
            for (int key = N; key < 2 * N; key++) {
                map.put(TestRecord.createRecord(key, "b"));
            }
            map.put(TestRecord.createRecord(N/2, "c"));
            map.delete(new TestKey(N + N/2));
            db.commitTransaction();
        }
        {
            // Force consolidation
            ((DatabaseOnDisk)db).consolidateAll();
        }
        //
        Cursor cursor = map.first();
        TestRecord record;
        while ((record = (TestRecord) cursor.next()) != null) {
            System.out.println(record);
        }
    }

    private static final File DB_DIRECTORY = new File("/tmp/erdo");
}
