/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeCursor extends MapCursor
{
    // Cursor interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        LazyRecord next = null;
        if (root != null) {
            next = root.record;
            root.promote();
        }
        if (next == null) {
            close();
        }
        return next;
    }

    @Override
    public void close()
    {
        if (inputs != null) {
            for (MapCursor input : inputs) {
                input.close();
            }
            inputs = null;
            root = null;
        }
    }

    // MergeCursor interface

    public void addInput(MapCursor input)
    {
        inputs.add(input);
    }

    public void start() throws IOException, InterruptedException
    {
        // Number of nodes at leaf level is the smallest power of 2 >= inputs.size().
        int nLeaves = 1;
        while (nLeaves < inputs.size()) {
            nLeaves *= 2;
        }
        int nNodes = 2 * nLeaves - 1;
        firstLeaf = nNodes / 2;
        // Create tree
        root = createNode(0);
        // Move records up the tree
        root.prime();
    }

    public MergeCursor(Merger merger)
    {
        super(null, null);
        this.merger = merger;
    }

    // For use by this package

    Node mergeNode(int position, Node left, Node right)
    {
        return new MergeNode(this, position, left, right);
    }

    Node inputNode(int position, MapCursor input)
    {
        return new InputNode(position, input);
    }

    Node fillerNode(int position)
    {
        return new FillerNode(position);
    }

    // For use by this class

    private Node createNode(int position)
    {
        // positionInFile: Refers to positionInFile in a breadth-first traversal of the tree.
        return
            position < firstLeaf
            ? mergeNode(position, createNode(2 * position + 1), createNode(2 * position + 2))
            : position < firstLeaf + inputs.size()
              ? inputNode(position, inputs.get(position - firstLeaf))
              : fillerNode(position);
    }

    // State

    final Merger merger;
    private List<MapCursor> inputs = new ArrayList<>();
    private int firstLeaf;
    private Node root;
}
