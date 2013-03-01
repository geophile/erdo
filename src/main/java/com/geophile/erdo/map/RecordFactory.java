package com.geophile.erdo.map;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.AbstractRecord;
import com.geophile.erdo.UsageError;

public class RecordFactory
{
    public AbstractKey newKey()
    {
        try {
            return keyClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UsageError(e);
        }
    }

    public AbstractRecord newRecord()
    {
        try {
            return recordClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UsageError(e);
        }
    }

    public RecordFactory(String keyClassName, String recordClassName)
    {
        try {
            this.keyClass = (Class<AbstractKey>) Class.forName(keyClassName);
            this.recordClass = (Class<AbstractRecord<? extends AbstractKey>>) Class.forName(recordClassName);
        } catch (ClassNotFoundException e) {
            throw new UsageError(e);
        }
    }

    public RecordFactory(Class<? extends AbstractKey> keyClass,
                         Class<? extends AbstractRecord<? extends AbstractKey>> recordClass)
    {
        this.keyClass = keyClass;
        this.recordClass = recordClass;
    }

    private Class<? extends AbstractKey> keyClass;
    private Class<? extends AbstractRecord<? extends AbstractKey>> recordClass;
}
