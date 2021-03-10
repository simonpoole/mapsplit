package dev.osm.mapsplit;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.junit.Test;

public class CheckAndFillTest {

    private static final Logger LOGGER = Logger.getLogger(CheckAndFillTest.class.getName());


    /**
     * Basic test to check that stuff happens
     */
    @Test
    public void fill() {
       
        List<Long> tiles = new ArrayList<>();
        
        int c = 13 << Const.MAX_ZOOM | 12;
        tiles.add(((long) c) << HeapMap.TILE_Y_SHIFT);
        
        int c2 = 15 << Const.MAX_ZOOM | 15;
        tiles.add(((long) c2) << HeapMap.TILE_Y_SHIFT);
        
        MapSplit mapsplit = new MapSplit(16, null, new int[] { Const.NODE_MAP_SIZE, Const.WAY_MAP_SIZE, Const.RELATION_MAP_SIZE }, 2000, 1.0, null, false, false);
        mapsplit.checkAndFill(tiles);
        
        for (long t:tiles) {
            int tx = mapsplit.nmap.tileX(t);
            int ty = mapsplit.nmap.tileY(t);
            System.out.println(tx + " " + ty);
        }
    }


}
