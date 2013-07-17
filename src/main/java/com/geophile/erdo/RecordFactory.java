/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.geophile.erdo;

import com.geophile.erdo.map.SimpleRecordFactory;

import java.io.Serializable;

public abstract class RecordFactory implements Serializable
{
    public abstract AbstractKey newKey();

    public abstract AbstractRecord newRecord();

    public static RecordFactory simpleRecordFactory(Class<? extends AbstractKey> keyClass,
                                                    Class<? extends AbstractRecord> recordClass)
    {
        return new SimpleRecordFactory(keyClass, recordClass);
    }
}
