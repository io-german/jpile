package com.opower.persistence.jpile.reflection;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * Test caching to make sure it works correctly
 *
 * @author amir.raminfar
 */
public class CachedProxyTest {
    private static final String FOO = "foo";
    private static final String BAR = "bar";

    private MyTestClass theImplementation = new MyTestClass();
    private MyTestClass cached = CachedProxy.create(theImplementation);

    @Test
    public void testWithVarArray() {
        assertEquals(FOO, cached.doSomethingWithVarParams(1, true, "test"));
        cached.doSomethingWithVarParams(1, true, "test"); // Call it again
        assertEquals(1, theImplementation.doSomethingWithVarParams);
    }

    @Test
    public void testWithSingleParam() {
        assertEquals(BAR, cached.doSomethingWithObject(FOO));
        cached.doSomethingWithObject(FOO);
        assertEquals(1, theImplementation.doSomethingWithObject);
    }

    @Test
    public void testReturningNull() {
        assertNull(cached.returnsNull(FOO));
        cached.returnsNull(FOO);
        assertEquals(1, theImplementation.returnsNull);
    }

    /**
     * A fake class for testing.
     */
    public static class MyTestClass {
        int doSomethingWithVarParams = 0;
        int doSomethingWithObject = 0;
        int returnsNull = 0;

        Object doSomethingWithVarParams(Object... args) {
            doSomethingWithVarParams++;
            return FOO;
        }

        Object doSomethingWithObject(Object o) {
            doSomethingWithObject++;
            return BAR;
        }

        Object returnsNull(Object... args) {
            returnsNull++;
            return null;
        }
    }
}
