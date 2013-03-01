package com.geophile.erdo.apiimpl;

import com.geophile.erdo.AbstractKey;
import com.geophile.erdo.Keys;

public class KeyRange extends Keys
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s%s,%s%s",
                             loInclusive ? '[' : '(',
                             lo,
                             hi,
                             hiInclusive ? ']' : ')');
    }

    // KeyRange interface

    public AbstractKey lo()
    {
        return lo;
    }

    public AbstractKey hi()
    {
        return hi;
    }

    public boolean loInclusive()
    {
        return loInclusive;
    }

    public boolean hiInclusive()
    {
        return hiInclusive;
    }

    public void erdoId(int erdoId)
    {
        if (lo != null) {
            lo.erdoId(erdoId);
        }
        if (hi != null) {
            hi.erdoId(erdoId);
        }
        assert lo == null || hi == null || lo.compareTo(hi) <= 0 : String.format("%s:%s", lo, hi);
    }

    /**
     * Return a value indicating the relationship of key to this KeyRange. The return value is KEY_BEFORE_RANGE if
     * the key is outside the keyRange and key <= lo(). The return value is KEY_AFTER_RANGE if the key is outside the
     * keyRange and key >= hi(). Otherwise, the return value is KEY_IN_RANGE.
     *
     * @param key The key to be classified.
     * @return KEY_BEFORE_RANGE if the key is below the keyRange, KEY_AFTER_RANGE if it is above the keyRange,
     *         KEY_IN_RANGE otherwise.
     */
    public int classify(AbstractKey key)
    {
        int c;
        c = lo == null ? 1 : key.compareTo(lo);
        if (c < 0 || c == 0 && !loInclusive) {
            c = KEY_BEFORE_RANGE;
        } else {
            c = hi == null ? -1 : key.compareTo(hi);
            if (c > 0 || c == 0 && !hiInclusive) {
                c = KEY_AFTER_RANGE;
            } else {
                c = KEY_IN_RANGE;
            }
        }
        return c;
    }

    public boolean singleKey()
    {
        if (!singleKeySet) {
            if (lo == null || hi == null) {
                singleKey = false;
            } else if (lo == hi || lo.compareTo(hi) == 0) {
                assert loInclusive && hiInclusive : this;
                singleKey = true;
            } else {
                singleKey = false;
            }
            singleKeySet = true;
        }
        return singleKey;
    }

    public KeyRange(AbstractKey lo, boolean loInclusive, AbstractKey hi, boolean hiInclusive)
    {
        assert !(lo == null && loInclusive) : lo;
        assert !(hi == null && hiInclusive) : hi;
        this.lo = lo;
        this.loInclusive = loInclusive;
        this.hi = hi;
        this.hiInclusive = hiInclusive;
    }

    // Class state

    public static final int KEY_BEFORE_RANGE = -1;
    public static final int KEY_IN_RANGE = 0;
    public static final int KEY_AFTER_RANGE = 1;

    // Object state

    private final AbstractKey lo;
    private final AbstractKey hi;
    private final boolean loInclusive;
    private final boolean hiInclusive;
    private boolean singleKey;
    private boolean singleKeySet = false;
}
