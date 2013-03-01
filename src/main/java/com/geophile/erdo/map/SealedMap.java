package com.geophile.erdo.map;

import com.geophile.erdo.consolidate.Consolidation;

/**
 * Base class for sealed maps
 */
public interface SealedMap
    extends SealedMapOperations,
            CommonMapOperations,
            Consolidation.Element
{
}
