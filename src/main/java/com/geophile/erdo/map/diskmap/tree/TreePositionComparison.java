package com.geophile.erdo.map.diskmap.tree;

enum TreePositionComparison
{
    // Why no ==: Because by the time we are searching a tree, key = x has been turned into key >=x and key <= x.
    // Why no !=: Because != searches aren't supported.

    LT("<"),
    LE("<="),
    GT(">"),
    GE(">=");

    public String toString()
    {
        return symbol;
    }

    private TreePositionComparison(String symbol)
    {
        this.symbol = symbol;
    }

    private final String symbol;
}
