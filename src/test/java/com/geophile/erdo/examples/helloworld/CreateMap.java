package com.geophile.erdo.examples.helloworld;

import com.geophile.erdo.Database;

import java.io.File;
import java.io.IOException;

public class CreateMap
{
    public static void main(String[] args) throws IOException, InterruptedException
    {
        Database db = Database.openDatabase(DB_DIRECTORY);
        db.createMap("musicians", Name.class, Person.class);
        db.close();
        System.out.println(String.format("Created 'musicians' map in database %s", DB_DIRECTORY));
    }

    private static final File DB_DIRECTORY = new File("/tmp/mydb");
}
