package com.geophile.erdo.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public interface Transferrable
{
    void writeTo(ByteBuffer buffer) throws BufferOverflowException;
    void readFrom(ByteBuffer buffer);
    int recordCount();
}
