package dev.osm.mapsplit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class ArrayMapTest {

    @Test
    public void testPutUpdateAndRetrieve() {

        var map = new ArrayMap(100);

        int tileX = 1245;
        int tileY = 99;

        /* put some entries into the tile and its eastern neighbor */

        map.put(42, tileX , tileY, OsmMap.NEIGHBOURS_NONE);
        map.put(0, tileX, tileY, OsmMap.NEIGHBOURS_EAST);
        map.put(100, tileX + 1, tileY, OsmMap.NEIGHBOURS_NONE);

        /* retrieve the values and tile lists from the map */

        long value42 = map.get(42);
        assertEquals(tileX, map.tileX(value42));
        assertEquals(tileY, map.tileY(value42));

        assertEquals(List.of(tileX << 16 | tileY), map.getAllTiles(42));
        assertEquals(2, map.getAllTiles(0).size());

        /* update one of the values and check again */

        map.update(42, List.of(
                map.createValue(tileX + 1, tileY, OsmMap.NEIGHBOURS_NONE),
                map.createValue(tileX, tileY + 1, OsmMap.NEIGHBOURS_NONE)));

        assertEquals(3, map.getAllTiles(42).size());

        /* check the keys */

        assertEquals(Set.of(0l, 42l, 100l), new HashSet<>(map.keys()));

    }

    /** tests handling of tile 0/0 */
    @Test
    public void testTile00() {

        var map = new ArrayMap(100);

        map.put(0, 0, 0, OsmMap.NEIGHBOURS_NONE);
        map.put(1, 0, 0, OsmMap.NEIGHBOURS_SOUTH_EAST);
        map.put(2, 1, 0, OsmMap.NEIGHBOURS_NONE);

        assertEquals(Set.of(0l, 1l, 2l), new HashSet<>(map.keys()));

        long value0 = map.get(0);
        assertEquals(0, map.tileX(value0));
        assertEquals(0, map.tileY(value0));
        assertEquals(List.of(0 << 16 | 0), map.getAllTiles(0));

        long value1 = map.get(0);
        assertEquals(0, map.tileX(value1));
        assertEquals(0, map.tileY(value1));
        assertEquals(4, map.getAllTiles(1).size());

        map.update(2, List.of(map.createValue(0, 0, OsmMap.NEIGHBOURS_NONE)));
        assertTrue(map.getAllTiles(0).contains(0 << 16 | 0));

    }

}
