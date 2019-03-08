package dev.osm.mapsplit;

public final class Const {

    static final String MBT_VERSION = "0.2.0";

    static final String OSM_ATTRIBUTION = "OpenStreetMap Contributors ODbL 1.0";

    static final String MSF_MIME_TYPE = "application/vnd.openstreetmap.data+pbf";

    /*
     * the maximum zoom-level, this limited currently by the fact that we pack the tile numbers in to one 32bit int
     */
    static final int MAX_ZOOM = 16;

    // used on values
    static final long MAX_TILE_NUMBER = (long) Math.pow(2, MAX_ZOOM) - 1;

    /**
     * Private constructor to stop instantiation
     */
    private Const() {
        // nothing
    }
}
