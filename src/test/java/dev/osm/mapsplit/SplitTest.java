package dev.osm.mapsplit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.logging.Logger;

import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.osmosis.OsmosisReader;

import ch.poole.geo.mbtiles4j.MBTilesReadException;
import ch.poole.geo.mbtiles4j.MBTilesReader;
import ch.poole.geo.mbtiles4j.Tile;
import ch.poole.geo.mbtiles4j.model.MetadataEntry;

public class SplitTest {

    private static final Logger LOGGER = Logger.getLogger(SplitTest.class.getName());

    private static final String MSF_OUTPUT_FILENAME = "build/tmp/tiles/liechtenstein.msf";
    private static final String MSF_OUTPUT_DIR      = "build/tmp/tiles/";
    private static final String TEST_DATA_PBF       = "test-data/liechtenstein-latest.osm.pbf";
    private static final long TEST_DATA_PBF_LATEST  = 1544288785000L; 

    private static final String TEST_DATA_EXTREME_NODES_PBF = "test-data/extreme-nodes.osm.pbf";
    private static final String TEST_DATA_BORDER_TEST_PBF = "test-data/border-test.osm.pbf";

    /**
     * Pre-test setup
     */
    @Before
    public void setup() {
        File output = new File(MSF_OUTPUT_FILENAME);
        output.delete();
        File path = new File(MSF_OUTPUT_DIR);
        path.mkdirs();
    }

    /**
     * Basic test to check that stuff happens
     */
    @Test
    public void split() {
        try {
            File output = new File(MSF_OUTPUT_FILENAME);
            Assert.assertFalse(output.exists());
            MapSplit.main(new String[] { "-mtvM", "-i", TEST_DATA_PBF, "-o", MSF_OUTPUT_FILENAME, "-f", "2000", "-z", "16", "-O", "2000" });
            checkMetadata(12, 16, TEST_DATA_PBF_LATEST);
            Tile t = getTile(15, 17250, 11506);
            Assert.assertNotNull(t);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Basic test to check that sparse tiles are optimized correctly
     */
    @Test
    public void splitOptimizeSparseTiles() {
        try {
            File output = new File(MSF_OUTPUT_FILENAME);
            Assert.assertFalse(output.exists());
            MapSplit.main(new String[] { "-mtvM", "-i", TEST_DATA_PBF, "-o", MSF_OUTPUT_FILENAME, "-f", "2000", "-z", "16", "-O", "20000000" });
            checkMetadata(12, 12, null);
            Tile t = getTile(12, 2143, 1439);
            Assert.assertNotNull(t);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Basic test to check that stuff happens, with full relations
     */
    @Test
    public void splitWithFullRelations() {
        try {
            File output = new File(MSF_OUTPUT_FILENAME);
            Assert.assertFalse(output.exists());
            MapSplit.main(new String[] { "-cmtvM", "-i", TEST_DATA_PBF, "-o", MSF_OUTPUT_FILENAME, "-f", "2000", "-z", "16", "-O", "2000" });
            checkMetadata(12, 16, TEST_DATA_PBF_LATEST);
            Tile t = getTile(16, 34501, 23013);
            Assert.assertNotNull(t);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Basic test, optimization turned of so that we can check clipping below
     */
    @Test
    public void splitNoOptimize() {
        try {
            File output = new File(MSF_OUTPUT_FILENAME);
            Assert.assertFalse(output.exists());
            MapSplit.main(new String[] { "-mtvM", "-i", TEST_DATA_PBF, "-o", MSF_OUTPUT_FILENAME, "-f", "2000", "-z", "16" });
            checkMetadata(16, 16, TEST_DATA_PBF_LATEST);
            Tile t = getTile(16, 34496, 23000);
            Assert.assertNotNull(t); // should exist, is outside of Vaduz
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Basic test for clipping
     */
    @Test
    public void splitAndClip() {
        try {
            File output = new File(MSF_OUTPUT_FILENAME);
            Assert.assertFalse(output.exists());
            MapSplit.main(new String[] { "-mtvM", "-p", "test-data/vaduz.poly", "-i", TEST_DATA_PBF, "-o", MSF_OUTPUT_FILENAME, "-f", "2000", "-z", "16" });
            checkMetadata(16, 16, TEST_DATA_PBF_LATEST);
            Tile t = getTile(16, 33972, 23225);
            Assert.assertNull(t); // shouldn't exist, is outside of Vaduz
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test for nodes with very small or large lat/lon values
     */
    @Test
    public void splitNearLatLonBounds() {
        try {
            File output = new File(MSF_OUTPUT_FILENAME);
            Assert.assertFalse(output.exists());
            MapSplit.main(new String[] { "-mtvM", "-i", TEST_DATA_EXTREME_NODES_PBF,
                    "-o", MSF_OUTPUT_FILENAME, "-z", "13"});
            checkMetadata(13, 13, null);
            assertEquals(1, getTileData(13, 0, 0).size());
            assertEquals(1, getTileData(13, 0, 8191).size());
            assertEquals(1, getTileData(13, 8191, 0).size());
            assertEquals(1, getTileData(13, 8191, 8191).size());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test for a node near the tile's borders (to see if neighbor information works correctly)
     */
    @Test
    public void splitBorderTest() throws Exception {
        File output = new File(MSF_OUTPUT_FILENAME);
        Assert.assertFalse(output.exists());
        MapSplit.main(new String[] { "-mtvM", "-i", TEST_DATA_BORDER_TEST_PBF, "-o", MSF_OUTPUT_FILENAME,
                "-z", "1", "-b", "0.1"});
        checkMetadata(1, 1, null);
        assertNotNull(getTile(1, 0, 0));
        assertEquals(1, getTileData(1, 0, 0).size());
        assertNotNull(getTile(1, 1, 0));
        assertEquals(1, getTileData(1, 1, 0).size());
        assertNotNull(getTile(1, 0, 1));
        assertEquals(1, getTileData(1, 0, 1).size());
        assertNotNull(getTile(1, 1, 1));
        assertEquals(1, getTileData(1, 1, 1).size());
    }

    /**
     * Check that the metadata in the MBTiles is what we expect
     * 
     * @param min min zoom in metadata
     * @param max max zoom in metadata
     * @param date latest date in metadata, or null to skip this check
     * @throws MBTilesReadException if we can't read or access the MBTiles file
     */
    private void checkMetadata(int min, int max, @Nullable Long date) throws MBTilesReadException {
        MBTilesReader r = null;
        try {
            r = new MBTilesReader(new File(MSF_OUTPUT_FILENAME));
            MetadataEntry md = r.getMetadata();
            final Map<String, String> required = new HashMap<>();
            md.getRequiredKeyValuePairs().forEach(new Consumer<Entry<String, String>>() {

                @Override
                public void accept(Entry<String, String> e) {
                    required.put(e.getKey(), e.getValue());
                }

            });
            final Map<String, String> custom = new HashMap<>();
            md.getCustomKeyValuePairs().forEach(new Consumer<Entry<String, String>>() {

                @Override
                public void accept(Entry<String, String> e) {
                    custom.put(e.getKey(), e.getValue());
                }

            });
            Assert.assertEquals(Const.MSF_MIME_TYPE, required.get("format"));
            Assert.assertEquals(min, r.getMinZoom());
            Assert.assertEquals(max, r.getMaxZoom());
            if (date != null) {
                Assert.assertEquals((long)date, Long.parseLong(custom.get("latest_date")));
            }
        } finally {
            if (r != null) {
                r.close();
            }
        }
    }

    /**
     * Get a tile from a MSF format file
     * 
     * @param z zoom
     * @param x x tile number
     * @param y y tile number in google/OSM schema
     * @return a Tile or null if not found
     */
    @Nullable
    private Tile getTile(int z, int x, int y) {
        MBTilesReader r = null;
        int tmsY = Integer.MIN_VALUE;
        try {
            r = new MBTilesReader(new File(MSF_OUTPUT_FILENAME));
            int ymax = 1 << z;
            tmsY = ymax - y - 1; // TMS scheme
            return r.getTile(z, x, tmsY);
        } catch (MBTilesReadException e) {
            LOGGER.warning(e.getMessage() + " z:" + z + " x:" + x + " y:" + y + "/" + tmsY);
            return null;
        } finally {
            if (r != null) {
                r.close();
            }
        }
    }

    /**
     * Get a tile from a MSF format file
     * 
     * @param z zoom
     * @param x x tile number
     * @param y y tile number in google/OSM schema
     * @return the data or null if not found
     */
    @Nullable
    private Collection<Entity> getTileData(int z, int x, int y) {

        Tile tile = getTile(z, x, y);

        if (tile == null) {
            return null;
        } else {

            Collection<Entity> result = new ArrayList<>();

            OsmosisReader reader = new OsmosisReader(tile.getData());
            reader.setSink(new Sink() {
                @Override
                public void process(EntityContainer entityContainer) {
                    if (!(entityContainer.getEntity() instanceof Bound)) {
                        result.add(entityContainer.getEntity());
                    }
                }
                @Override public void initialize(Map<String, Object> metaData) {}
                @Override public void complete() {}
                @Override public void close() {}
            });
            reader.run();

            return result;

        }

    }

}
