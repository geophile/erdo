/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.map.diskmap.tree;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.map.MapCursor;
import com.geophile.erdo.map.diskmap.IndexRecord;
import com.geophile.erdo.map.mergescan.AbstractMultiRecord;
import com.geophile.erdo.map.mergescan.MultiRecordKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LevelOneMultiRecord extends AbstractMultiRecord
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("multirecord(%s: %s)", loZeroPosition.segment(), key());
    }

    // Transferrable interface

    @Override
    public void writeTo(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFrom(ByteBuffer buffer)
    {
        throw new UnsupportedOperationException();
    }

    // AbstractMultiRecord interface

    @Override
    public void append(AbstractRecord record)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapCursor cursor()
    {
        LOG.log(Level.INFO,
                "{0}: Record cursor: {1} - {2}",
                new Object[]{this, loZeroPosition, hiZeroPosition});
        return new LevelOneMultiRecordCursor(loZeroPosition, hiZeroPosition);
    }

    // LazyRecord interface (key() is provided by AbstractRecord)

    @Override
    public ByteBuffer keyBuffer() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractRecord materializeRecord() throws IOException, InterruptedException
    {
        return this;
    }

    @Override
    public ByteBuffer recordBuffer() throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean prefersSerialized()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroyRecordReference()
    {
        loOnePosition.destroyRecordReference();
        hiOnePosition.destroyRecordReference();
        loZeroPosition.destroyRecordReference();
        hiZeroPosition.destroyRecordReference();
    }

    // LevelOneMultiRecord interface

    public MapCursor levelOneScan() throws IOException, InterruptedException
    {
        LOG.log(Level.INFO,
                "{0}: Level one cursor: {1} - {2}",
                new Object[]{this, loOnePosition, hiOnePosition});
        return new LevelOneMultiRecordCursor(loOnePosition, hiOnePosition);
    }

    public TreeSegment leafSegment()
    {
        return loZeroPosition.segment();
    }

    public LevelOneMultiRecord(TreePosition loOnePosition,
                               IndexRecord loIndexRecord,
                               TreePosition hiOnePosition,
                               IndexRecord hiIndexRecord)
        throws IOException, InterruptedException
    {
        super(new MultiRecordKey(loIndexRecord.key(),
                                 hiIndexRecord == null
                                 ? null
                                 : lastKeyInLeafSegment(loOnePosition.tree(), hiIndexRecord.childPageAddress())));
        Tree tree = loOnePosition.tree();
        assert hiOnePosition.tree() == tree : this;
        assert loOnePosition.level().levelNumber() == 1 : this;
        assert hiOnePosition.level().levelNumber() == 1 : this;
        this.loOnePosition = loOnePosition.copy();
        this.hiOnePosition = hiOnePosition.copy();
        this.loZeroPosition =
            tree.newPosition().level(0).pageAddress(loIndexRecord.childPageAddress()).goToFirstRecordOfPage();
        this.hiZeroPosition =
            this.loZeroPosition.copy().goToLastPageOfSegment().goToLastRecordOfPage();
        LOG.log(Level.INFO,
                "Creating multirecord {0}. File bounds {1} : {2}, index bounds {3} : {4}",
                new Object[]{this,
                             this.loZeroPosition,
                             this.hiZeroPosition,
                             this.loOnePosition,
                             this.hiOnePosition});
    }

        // For use by this class

    private static AbstractKey lastKeyInLeafSegment(Tree tree, int pageAddress)
    {
        return tree.level(0).segment(tree.segmentNumber(pageAddress)).leafLastKey();
    }

    // Class state

    private static final Logger LOG = Logger.getLogger(LevelOneMultiRecord.class.getName());

    // Object state

    private final TreePosition loOnePosition;
    private final TreePosition hiOnePosition;
    private final TreePosition loZeroPosition;
    private final TreePosition hiZeroPosition;
}
