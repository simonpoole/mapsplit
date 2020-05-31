package dev.osm.mapsplit;

import static org.junit.Assert.assertEquals;

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

}
