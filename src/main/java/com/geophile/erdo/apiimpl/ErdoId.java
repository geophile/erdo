/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.apiimpl;

import com.geophile.erdo.AbstractKey;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

// Used to specify the lower bound of an unrestricted OrderedMap scan. Will never be serialized.

public class ErdoId extends AbstractKey
{
    // AbstractKey interface

    @Override
    public void readFrom(ByteBuffer buffer) throws BufferUnderflowException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(ByteBuffer buffer) throws BufferOverflowException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int estimatedSizeBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractKey copy()
    {
        throw new UnsupportedOperationException();
    }

    // ErdoId interface

    public ErdoId(int erdoId)
    {
        erdoId(erdoId);
    }
}
