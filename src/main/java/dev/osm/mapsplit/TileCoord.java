package dev.osm.mapsplit;

/**
 * provides utility methods for representing a tile coordinate as an int
 */
public final class TileCoord {

    /** prevents instantiation */
    private TileCoord() {}

    public static final int encode(int x, int y) {
        return x << Const.MAX_ZOOM | y;
    }

    public static final int decodeX(int tileCoord) {
        return tileCoord >>> Const.MAX_ZOOM;
    }

    public static final int decodeY(int tileCoord) {
        return (int)(tileCoord & Const.MAX_TILE_NUMBER);
    }

}
