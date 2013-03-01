/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public interface Transferrable
{
    void writeTo(ByteBuffer buffer) throws BufferOverflowException;
    void readFrom(ByteBuffer buffer);
    int recordCount();
}
