package com.geophile.erdo.map.mergescan;

class FastFillerNode extends FastNode
{
    public String toString()
    {
        return String.format("Filler(#%s)", position);
    }

    public void prime()
    {
    }

    public void fastPromote()
    {
    }

    public FastFillerNode(int position)
    {
        super(position);
    }
}
