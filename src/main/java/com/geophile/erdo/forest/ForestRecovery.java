package com.geophile.erdo.forest;

import com.geophile.erdo.apiimpl.DatabaseImpl;

import java.io.IOException;

public interface ForestRecovery
{
    Forest recoverForest(DatabaseImpl database) throws IOException, InterruptedException;
}
