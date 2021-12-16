package dev.osm.mapsplit;

/*
 * Mapsplit - A simple but fast tile splitter for large OSM data
 * 
 * Written in 2011 by Peda (osm-mapsplit@won2.de)
 * 
 * To the extent possible under law, the author(s) have dedicated all copyright and related and neighboring rights to
 * this software to the public domain worldwide. This software is distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software. If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

import java.util.Collection;
import java.util.List;
import java.util.stream.LongStream;

/**
 * This is the central data structure of Mapsplit. It maps OSM element IDs to the tile(s) each element is located in. As
 * there are three types of OSM elements (node/way/relation), there will be three instances of this kind of data
 * structure.
 */
public interface OsmMap {

    /** no neighbour */
    public static final int NEIGHBOURS_NONE = 0;

    /** we have a neighbour in the right (east) */
    public static final int NEIGHBOURS_EAST = 1;

    /** we have a neighbour at the bottom (south) */
    public static final int NEIGHBOURS_SOUTH = 2;

    /**
     * we have neighbours in the east, the south, and the south-east. It's not possible for a node to be in the south
     * and east tile, but not in the south-east tile.
     */
    public static final int NEIGHBOURS_SOUTH_EAST = 3;

    /**
     * put a new value into the hash map at the given key (entity id)
     * 
     * @param key the entries key (ID)
     * @param tileX x tile number
     * @param tileY y tile number
     * @param neighbours bit encoded neighbours
     */
    public abstract void put(long key, int tileX, int tileY, int neighbours);

    /**
     * returns an entry of the hashmap at the given key or 0 if not found. The return value is encoded in an internal
     * format and has to be parsed with the methods provided by this interface.
     * 
     * @param key the entries key (ID)
     * @return an entry of the hashmap at the given key or 0 if not found
     */
    public abstract long get(long key);

    /**
     * update key so it knows about all tiles where it is needed. The given longs in the list are expected in this map's
     * format. Neighbourhood of the tile-IDs are evaluated.
     * 
     * @param key the entries key (ID)
     * @param tiles a Collection of tiles in encoded form including neighbour bits
     */
    public abstract void update(long key, Collection<Long> tiles);

    /**
     * variant of {@link #update(long, Collection)} that takes individual tile coords (encoded with TileCoord) without
     * neighbor information
     * 
     * @param key the entries key (ID)
     * @param tiles a Collection of tiles in encoded form without neighbour bits
     */
    public abstract void updateInt(long key, Collection<Integer> tiles);

    /**
     * returns a list of all tiles this key is in. This contains the base-tile, neighbours and tiles where this key is
     * connected to other keys, e.g. by a way. The format of the integer is (tileX << 16 | tileY).
     * 
     * @param key the node, way or relation we're looking at
     * @return a list of all tiles where this key is used
     */
    public abstract List<Integer> getAllTiles(long key);

    /**
     * for debugging this method tells you how much of the buckets are used. A load < 0,5 is desirable for good speed
     * 
     * @return the load of the map
     */
    public default double getLoad() {
        return keys().count() / (double) getCapacity();
    }

    /**
     * for debugging this method gives the ratio of misses to hits. For open addressing multiple misses may occur. The
     * ratio gives feedback about the goodness of the data/hash function. A value of 0 means perfect hashing, a value
     * larger 2 means a high load or a bad hash function.
     * 
     * @return the hit miss ratio
     */
    public abstract double getMissHitRatio();

    /**
     * return the tilenumber in west-east-direction encoded in the given value
     * 
     * @param value the value stored in the Map
     * @return the tilenumber in west-east-direction
     */
    public int tileX(long value);

    /**
     * return the tilenumber in north-south-direction encoded in the given value
     * 
     * @param value the value stored in the Map
     * @return the tilenumber in north-south-direction
     */
    public int tileY(long value);

    /**
     * return the neighbourhood information encoded in the given value
     * 
     * @param value the value stored in the Map
     * @return the neighbourhood information
     */
    public int neighbour(long value);

    /**
     * return the capacity of the map, i.e. the max number of elements that would fit in that map
     * 
     * @return the capacity of this map
     */
    public long getCapacity();

    /**
     * return all the keys. Implemented using a stream instead of a collection to avoid having to keep them all in
     * memory at the same time. (There can be a lot of them!)
     * 
     * @return a stream providing all the keys from the map
     */
    public LongStream keys();
}
