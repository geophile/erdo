package com.geophile.erdo.map.mergescan;

import com.geophile.erdo.map.LazyRecord;
import com.geophile.erdo.map.MapScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeScan extends MapScan
{
    // Scan interface

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
            for (MapScan input : inputs) {
                input.close();
            }
            inputs = null;
            root = null;
        }
    }

    // MergeScan interface

    public void addInput(MapScan input)
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

    public MergeScan(Merger merger)
    {
        this.merger = merger;
    }

    // For use by this package

    Node mergeNode(int position, Node left, Node right)
    {
        return new MergeNode(this, position, left, right);
    }

    Node inputNode(int position, MapScan input)
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
    private List<MapScan> inputs = new ArrayList<>();
    private int firstLeaf;
    private Node root;
}
