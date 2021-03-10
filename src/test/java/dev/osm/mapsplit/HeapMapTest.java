package dev.osm.mapsplit;

import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import org.junit.Test;

public class HeapMapTest {

    private static final Logger LOGGER = Logger.getLogger(HeapMapTest.class.getName());

    /**
     * Basic test to check that stuff happens
     */
    @Test
    public void putAndGetTest() {
        OsmMap map = new HeapMap(100);
        final int testDataSize = 1000;
        long[] keys = new long[testDataSize];
        int[] x = new int[testDataSize];
        int[] y = new int[testDataSize];
        int[] n = new int[testDataSize];
        for (int i = 0; i < testDataSize; i++) {
            keys[i] = (long) (Math.random() * Long.MAX_VALUE);
            x[i] = (int) (Math.random() * Integer.MAX_VALUE) & (int) Const.MAX_TILE_NUMBER;
            y[i] = (int) (Math.random() * Integer.MAX_VALUE) & (int) Const.MAX_TILE_NUMBER;
            n[i] = (int) (Math.random() * Integer.MAX_VALUE) & 0x3;
            map.put(keys[i], x[i], y[i], n[i]);
        }
        assertEquals(1600, map.getCapacity());
        for (int i = 0; i < testDataSize; i++) {
            long value = map.get(keys[i]);
            assertEquals(x[i], map.tileX(value));
            assertEquals(y[i], map.tileY(value));
            assertEquals(n[i], map.neighbour(value));
        }
    }
}
