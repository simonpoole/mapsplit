package dev.osm.mapsplit;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class TileCoordTest {

    @Test
    public void testReversibility() {

        Random random = new Random(0);

        for (int i = 0; i < 500; i++) {

            int x = random.nextInt((int)Const.MAX_TILE_NUMBER + 1);
            int y = random.nextInt((int)Const.MAX_TILE_NUMBER + 1);

            int tileCoord = TileCoord.encode(x, y);
            assertEquals(x, TileCoord.decodeX(tileCoord));
            assertEquals(y, TileCoord.decodeY(tileCoord));

        }

    }

}
