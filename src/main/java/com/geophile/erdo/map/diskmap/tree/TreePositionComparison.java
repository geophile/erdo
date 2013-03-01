/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

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
