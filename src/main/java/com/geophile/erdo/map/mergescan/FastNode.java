package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.map.MapScan;

import java.io.IOException;

abstract class FastNode extends Node
{
    public final void promote() throws IOException, InterruptedException
    {
        if (multiRecordScan == null) {
            fastPromote();
        } else {
            record = multiRecordScan.next();
            if (record == null) {
                multiRecordScan = null;
                fastPromote();
            }
        }
        if (record != null) {
            key = record.key();
        }
    }

    public abstract void fastPromote() throws IOException, InterruptedException;

    FastNode(int position)
    {
        super(position);
    }

    protected final void goSlow() throws IOException, InterruptedException
    {
        if (record instanceof AbstractMultiRecord) {
            multiRecordScan = ((AbstractMultiRecord) record).scan();
            record = multiRecordScan.next();
            if (record != null) {
                key = record.key();
            }
        }
    }

    // Object state

    private MapScan multiRecordScan;
}
