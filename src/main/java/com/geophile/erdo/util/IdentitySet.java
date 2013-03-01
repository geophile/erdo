package com.geophile.erdo.util;

import java.util.IdentityHashMap;

public class IdentitySet<T> extends IdentityHashMap<T, T>
{
    public T add(T t)
    {
        return put(t, t);
    }
}
