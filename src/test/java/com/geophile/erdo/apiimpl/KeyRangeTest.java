package com.geophile.erdo.apiimpl;

import com.geophile.erdo.Keys;
import com.geophile.erdo.TestKey;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class KeyRangeTest
{
    @Before
    public void before()
    {
        TestKey.testErdoId(1);
    }

    @Test
    public void testEq()
    {
        assertEquals(-1, classify(Keys.eq(key(5)), 4));
        assertEquals(0, classify(Keys.eq(key(5)), 5));
        assertEquals(1, classify(Keys.eq(key(5)), 6));
    }

    @Test(expected = AssertionError.class)
    public void testNullEq()
    {
        Keys.eq(null);
    }

    @Test
    public void testLt()
    {
        assertEquals(0, classify(Keys.lt(key(5)), 4));
        assertEquals(1, classify(Keys.lt(key(5)), 5));
        assertEquals(1, classify(Keys.lt(key(5)), 6));
    }

    @Test
    public void testNullLt()
    {
        assertEquals(0, classify(Keys.lt(null), 4));
    }

    @Test
    public void testLe()
    {
        assertEquals(0, classify(Keys.le(key(5)), 4));
        assertEquals(0, classify(Keys.le(key(5)), 5));
        assertEquals(1, classify(Keys.le(key(5)), 6));
    }

    @Test(expected = AssertionError.class)
    public void testNullLe()
    {
        Keys.le(null);
    }

    @Test
    public void testGt()
    {
        assertEquals(-1, classify(Keys.gt(key(5)), 4));
        assertEquals(-1, classify(Keys.gt(key(5)), 5));
        assertEquals(0, classify(Keys.gt(key(5)), 6));
    }

    @Test
    public void testNullGt()
    {
        assertEquals(0, classify(Keys.gt(null), 4));
    }

    @Test
    public void testGe()
    {
        assertEquals(-1, classify(Keys.ge(key(5)), 4));
        assertEquals(0, classify(Keys.ge(key(5)), 5));
        assertEquals(0, classify(Keys.ge(key(5)), 6));
    }

    @Test(expected = AssertionError.class)
    public void testNullGe()
    {
        Keys.ge(null);
    }

    @Test
    public void testGtLt()
    {
        assertEquals(-1, classify(Keys.gtlt(key(5), key(7)), 4));
        assertEquals(-1, classify(Keys.gtlt(key(5), key(7)), 5));
        assertEquals(0, classify(Keys.gtlt(key(5), key(7)), 6));
        assertEquals(1, classify(Keys.gtlt(key(5), key(7)), 7));
        assertEquals(1, classify(Keys.gtlt(key(5), key(7)), 8));
    }

    @Test
    public void testGtLtNull()
    {
        assertEquals(-1, classify(Keys.gtlt(key(5), null), 4));
        assertEquals(-1, classify(Keys.gtlt(key(5), null), 5));
        assertEquals(0, classify(Keys.gtlt(key(5), null), 6));
    }

    @Test
    public void testGtNullLt()
    {
        assertEquals(0, classify(Keys.gtlt(null, key(7)), 6));
        assertEquals(1, classify(Keys.gtlt(null, key(7)), 7));
        assertEquals(1, classify(Keys.gtlt(null, key(7)), 8));
    }

    @Test
    public void testGtLe()
    {
        assertEquals(-1, classify(Keys.gtle(key(5), key(7)), 4));
        assertEquals(-1, classify(Keys.gtle(key(5), key(7)), 5));
        assertEquals(0, classify(Keys.gtle(key(5), key(7)), 6));
        assertEquals(0, classify(Keys.gtle(key(5), key(7)), 7));
        assertEquals(1, classify(Keys.gtle(key(5), key(7)), 8));
    }

    @Test(expected = AssertionError.class)
    public void testGtLeNull()
    {
        Keys.gtle(key(5), null);
    }

    @Test
    public void testGtNullLe()
    {
        assertEquals(0, classify(Keys.gtle(null, key(7)), 6));
        assertEquals(0, classify(Keys.gtle(null, key(7)), 7));
        assertEquals(1, classify(Keys.gtle(null, key(7)), 8));
    }

    @Test
    public void testGeLt()
    {
        assertEquals(-1, classify(Keys.gelt(key(5), key(7)), 4));
        assertEquals(0, classify(Keys.gelt(key(5), key(7)), 5));
        assertEquals(0, classify(Keys.gelt(key(5), key(7)), 6));
        assertEquals(1, classify(Keys.gelt(key(5), key(7)), 7));
        assertEquals(1, classify(Keys.gelt(key(5), key(7)), 8));
    }

    @Test
    public void testGeLtNull()
    {
        assertEquals(-1, classify(Keys.gelt(key(5), null), 4));
        assertEquals(0, classify(Keys.gelt(key(5), null), 5));
        assertEquals(0, classify(Keys.gelt(key(5), null), 6));
    }

    @Test(expected = AssertionError.class)
    public void testGeNullLt()
    {
        Keys.gelt(null, key(7));
    }

    @Test
    public void testGeLe()
    {
        assertEquals(-1, classify(Keys.gele(key(5), key(7)), 4));
        assertEquals(0, classify(Keys.gele(key(5), key(7)), 5));
        assertEquals(0, classify(Keys.gele(key(5), key(7)), 6));
        assertEquals(0, classify(Keys.gele(key(5), key(7)), 7));
        assertEquals(1, classify(Keys.gele(key(5), key(7)), 8));
    }

    @Test(expected = AssertionError.class)
    public void testGeLeNull()
    {
        Keys.gele(key(5), null);
    }

    @Test(expected = AssertionError.class)
    public void testGeNullLe()
    {
        Keys.gele(null, key(7));
    }

    private int classify(Keys keys, int key)
    {
        return ((KeyRange) keys).classify(new TestKey(key));
    }

    private TestKey key(int key)
    {
        return new TestKey(key);
    }
}
