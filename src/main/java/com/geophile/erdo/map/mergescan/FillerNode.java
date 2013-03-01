package com.geophile.erdo.map.mergescan;

class FillerNode extends Node
{
    public String toString()
    {
        return String.format("Filler(#%s)", position);
    }

    public void prime()
    {
    }

    public void promote()
    {
    }

    public FillerNode(int position)
    {
        super(position);
    }
}
