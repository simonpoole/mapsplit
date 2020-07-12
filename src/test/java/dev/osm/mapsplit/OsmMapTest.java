package dev.osm.mapsplit;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OsmMapTest {

    /** provides an instance of {@link OsmMap} for us to test */
    public final Supplier<AbstractOsmMap> mapSupplier;

    public OsmMapTest(Supplier<AbstractOsmMap> mapSupplier) {
        this.mapSupplier = mapSupplier;
    }

    @Parameterized.Parameters
    public static Collection<Supplier<AbstractOsmMap>> testees() {
       return List.of(() -> new ArrayMap(100), () -> new HeapMap(100));
    }

    @Test
    public void testPutUpdateAndRetrieve() {

        AbstractOsmMap map = mapSupplier.get();

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

        assertEquals(Set.of(0l, 42l, 100l), map.keys().boxed().collect(toSet()));

    }

    /** tests handling of tile 0/0 */
    @Test
    public void testTile00() {

        AbstractOsmMap map = mapSupplier.get();

        map.put(0, 0, 0, OsmMap.NEIGHBOURS_NONE);
        map.put(1, 0, 0, OsmMap.NEIGHBOURS_SOUTH_EAST);
        map.put(2, 1, 0, OsmMap.NEIGHBOURS_NONE);

        assertEquals(Set.of(0l, 1l, 2l), map.keys().boxed().collect(toSet()));

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

    /** adds a lot of tiles to the same entry */
    @Test
    public void testLongTileLists() {

        List<Long> listOfTiles = new ArrayList<>();

        for (int i = 1; i < 1000; i++) {

            AbstractOsmMap map = mapSupplier.get();

            listOfTiles.add(map.createValue(10000 + i, 500, OsmMap.NEIGHBOURS_NONE));

            map.put(42, 10000, 500, OsmMap.NEIGHBOURS_NONE);
            map.update(42, new ArrayList<>(listOfTiles));

            assertEquals(1 + i, map.getAllTiles(42).size());

        }

    }

}
