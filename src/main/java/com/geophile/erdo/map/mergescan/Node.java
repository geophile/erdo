package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.map.LazyRecord;

import java.io.IOException;

abstract class Node
{
    public abstract void prime() throws IOException, InterruptedException;

    public abstract void promote() throws IOException, InterruptedException;

    public final void dump()
    {
        dump(0);
    }

    Node(int position)
    {
        this.position = position;
    }

    protected void dump(int level)
    {
        for (int i = 0; i < level; i++) {
            System.out.print("    ");
        }
        System.out.println(this);
    }

    // Object state

    protected final int position;
    protected AbstractKey key = null;
    protected LazyRecord record = null;
}
