package com.geophile.erdo.apiimpl;

import com.geophile.erdo.forest.Forest;
import com.geophile.erdo.map.Factory;

import java.io.IOException;

public class DisklessTestDatabase extends DatabaseImpl
{
    public DisklessTestDatabase(Factory factory) throws IOException, InterruptedException
    {
        super(factory);
        forest = Forest.create(this);
        factory.transactionManager(forest);
    }
}
