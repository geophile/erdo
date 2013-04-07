/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapCursor;
import com.geophile.erdo.map.forestmap.TimestampMerger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeCursor extends MapCursor
{
    // Cursor interface

    @Override
    public LazyRecord next() throws IOException, InterruptedException
    {
        assert forward;
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
    public LazyRecord previous() throws IOException, InterruptedException
    {
        assert !forward;
        LazyRecord previous = null;
        if (root != null) {
            previous = root.record;
            root.promote();
        }
        if (previous == null) {
            close();
        }
        return previous;
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

    public MergeCursor()
    {
        this(TimestampMerger.only(), true);
    }

    public MergeCursor(boolean forward)
    {
        this(TimestampMerger.only(), forward);
    }

    // For use by this package

    Node mergeNode(int position, Node left, Node right, boolean forward)
    {
        return new MergeNode(this, position, left, right, forward);
    }

    Node inputNode(int position, MapCursor input, boolean forward)
    {
        return new InputNode(position, input, forward);
    }

    Node fillerNode(int position)
    {
        return new FillerNode(position);
    }

    MergeCursor(Merger merger, boolean forward)
    {
        super(null, null);
        this.merger = merger;
        this.forward = forward;
    }

    // For use by this class

    private Node createNode(int position)
    {
        // positionInFile: Refers to positionInFile in a breadth-first traversal of the tree.
        return
            position < firstLeaf
            ? mergeNode(position, createNode(2 * position + 1), createNode(2 * position + 2), forward)
            : position < firstLeaf + inputs.size()
              ? inputNode(position, inputs.get(position - firstLeaf), forward)
              : fillerNode(position);
    }

    // Object state

    final Merger merger;
    private final boolean forward;
    private List<MapCursor> inputs = new ArrayList<>();
    private int firstLeaf;
    private Node root;
}
