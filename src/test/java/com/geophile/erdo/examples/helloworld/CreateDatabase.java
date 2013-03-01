package com.geophile.erdo.examples.helloworld;

import com.geophile.erdo.Database;

import java.io.File;
import java.io.IOException;

public class CreateDatabase
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        Database db = Database.createDatabase(DB_DIRECTORY);
        db.close();
        System.out.println(String.format("Database created in %s", DB_DIRECTORY));
    }

    private static final File DB_DIRECTORY = new File("/tmp/mydb");
}
