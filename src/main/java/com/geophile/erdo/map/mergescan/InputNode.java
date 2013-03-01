package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.map.MapScan;

import java.io.IOException;

class InputNode extends Node
{
    public String toString()
    {
        return String.format("InputNode(#%s: %s)", position, key);
    }

    public void prime() throws IOException, InterruptedException
    {
        promote();
    }

    public void promote() throws IOException, InterruptedException
    {
        record = input.next();
        key = record == null ? null : record.key();
    }

    public InputNode(int position, MapScan input)
    {
        super(position);
        this.input = input;
    }

    private final MapScan input;
}
