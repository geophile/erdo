package com.geophile.erdo.immutableitemcache;

class Id
{
    public String toString()
    {
        return String.format("id(%s)", value);
    }

    public int hashCode()
    {
        return value;
    }

    public boolean equals(Object o)
    {
        return o != null && o instanceof Id && ((Id)o).value == value;
    }

    public int value()
    {
        return value;
    }

    public Id(int value)
    {
        this.value = value;
    }

    private final int value;
}
