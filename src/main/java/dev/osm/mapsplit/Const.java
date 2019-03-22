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

    /*
     * the default sizes for the hash maps: should be a factor 2-4 of nodes in the pbf you want to read
     */
    static final int NODE_MAP_SIZE     = 60000000;
    static final int WAY_MAP_SIZE      = 10000000;
    static final int RELATION_MAP_SIZE = 2500000;

    /**
     * Private constructor to stop instantiation
     */
    private Const() {
        // nothing
    }
}
