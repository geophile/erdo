package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.AbstractKey;

public interface Merger
{
    Side merge(AbstractKey left, AbstractKey right);

    enum Side { LEFT, RIGHT }
}

