package com.geophile.erdo.apiimpl;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.OrderedMap;
import com.geophile.erdo.UsageError;
import com.geophile.erdo.forest.Forest;
import com.geophile.erdo.forest.ForestRecovery;
import com.geophile.erdo.map.Factory;
import com.geophile.erdo.map.diskmap.DBStructure;
import com.geophile.erdo.util.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class DatabaseOnDisk extends DatabaseImpl
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("Database(%s)", dbStructure.dbDirectory());
    }

    // Database interface

    public static synchronized DatabaseOnDisk createDatabase(File dbDirectory, Factory factory)
        throws IOException, InterruptedException
    {
        if (dbDirectory == null) {
            throw new IllegalArgumentException("dbDirectory must not be null.");
        }
        DBStructure dbStructure = new DBStructure(dbDirectory);
        FileUtil.ensureDirectoryExists(dbStructure.dbDirectory());
        FileUtil.ensureDirectoryExists(dbStructure.forestDirectory());
        FileUtil.ensureDirectoryExists(dbStructure.segmentsDirectory());
        FileUtil.ensureDirectoryExists(dbStructure.summariesDirectory());
        FileUtil.ensureDirectoryExists(dbStructure.mapsDirectory());
        FileUtil.checkFileDoesNotExist(dbStructure.dbPropertiesFile());
        ConfigurationImpl configuration = (ConfigurationImpl) factory.configuration();
        configuration.map.write(dbStructure.dbPropertiesFile());
        return new DatabaseOnDisk(factory, dbStructure, true);
    }

    public static synchronized DatabaseOnDisk openDatabase(File dbDirectory, Factory factory)
        throws IOException, InterruptedException
    {
        if (dbDirectory == null) {
            throw new IllegalArgumentException("dbDirectory must not be null.");
        }
        try {
            FileUtil.checkDirectoryExists(dbDirectory);
        } catch (IOException e) {
            throw new UsageError(e);
        }
        return new DatabaseOnDisk(factory, new DBStructure(dbDirectory), false);
    }

    // synchronized to prevent race condition when two threads create maps with the same name at the
    // same time.
    public synchronized OrderedMap createMap(String mapName,
                                             Class<? extends AbstractKey> keyClass,
                                             Class<? extends AbstractRecord<? extends AbstractKey>> recordClass)
        throws UsageError, IOException
    {
        OrderedMapImpl map = ((OrderedMapImpl) super.createMap(mapName, keyClass, recordClass));
        // Create file representing map
        File mapFile = new File(dbStructure.mapsDirectory(), Integer.toString(map.erdoId()));
        FileUtil.createFile(mapFile);
        FileWriter output = new FileWriter(mapFile);
        output.write(String.format("%s %s %s", mapName, keyClass.getName(), recordClass.getName()));
        output.close();
        return map;
    }

    @Override
    public void close() throws IOException, InterruptedException
    {
        super.close();
        FileUtil.deleteFile(dbStructure().pidFile());
    }

    // DatabaseOnDisk interface

    public DBStructure dbStructure()
    {
        return dbStructure;
    }

    public void consolidateAll() throws IOException, InterruptedException
    {
        forest.consolidateAll();
    }

    // For use by this package

    DatabaseOnDisk(Factory factory, DBStructure dbStructure, boolean create) throws IOException, InterruptedException
    {
        super(factory);
        this.dbStructure = dbStructure;
        writePID();
        if (create) {
            forest = Forest.create(this);
        } else {
            Map<String, Integer> mapNameToErdoId = new HashMap<>();
            File[] mapFiles = dbStructure.mapsDirectory().listFiles();
            assert mapFiles != null : dbStructure.mapsDirectory();
            for (File mapFile : mapFiles) {
                int erdoId = Integer.parseInt(mapFile.getName());
                BufferedReader input = new BufferedReader(new FileReader(mapFile));
                StringTokenizer tokenizer = new StringTokenizer(input.readLine());
                String mapName = tokenizer.nextToken();
                String keyClassName = tokenizer.nextToken();
                String recordClassName = tokenizer.nextToken();
                factory.registerKeyAndValueClasses(erdoId, keyClassName, recordClassName);
                mapNameToErdoId.put(mapName, erdoId);
            }
            forest = null;
            try {
                ForestRecovery forestRecovery =
                    (ForestRecovery) factory().forestRecoveryClass().newInstance();
                forest = forestRecovery.recoverForest(this);
            } catch (InstantiationException | IllegalAccessException e) {
                assert false : e;
            }
            assert forest != null;
            for (Map.Entry<String, Integer> entry : mapNameToErdoId.entrySet()) {
                maps.put(entry.getKey(),
                         new OrderedMapImpl(forest, entry.getValue()));
            }
        }
        factory.transactionManager(forest);
    }

    // For use by this class

    private void writePID() throws IOException
    {
        File pidFile = dbStructure.pidFile();
        if (pidFile.exists()) {
            ByteBuffer buffer = FileUtil.readFile(pidFile);
            String previousPID = new String(buffer.array(), 0, buffer.remaining());
            throw new UsageError(String.format("Erdo process with pid %s already running?", previousPID));
        } else {
            String pid = System.getProperty("pid");
            if (pid == null) {
                throw new UsageError("Set pid system property to the pid of the process starting erdo.");
            }
            ByteBuffer buffer = ByteBuffer.allocate(MAX_PID_SIZE);
            buffer.put(pid.getBytes());
            buffer.flip();
            FileUtil.writeFile(pidFile, buffer);
        }
    }

    // Class state

    private static final int MAX_PID_SIZE = 10;

    // Object state

    private final DBStructure dbStructure;
}
