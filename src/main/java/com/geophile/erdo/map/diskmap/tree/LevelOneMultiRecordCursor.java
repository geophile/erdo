/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap.tree;

class LevelOneMultiRecordCursor extends TreeLevelCursor
{
    @Override
    boolean atEnd()
    {
        return super.atEnd() || position.equals(stopPosition);
    }

    LevelOneMultiRecordCursor(TreePosition startPosition, TreePosition stopPosition)
    {
        super(startPosition);
        this.stopPosition = stopPosition;
    }

    private final TreePosition stopPosition;
}