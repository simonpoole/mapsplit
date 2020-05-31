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
 * 
 * Further work by Simon Poole 2018/19
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jetbrains.annotations.NotNull;

/**
 * An HashMap in Heap memory, optimized for size to use in OSM
 * 
 * See {@link AbstractOsmMap} for the meaning of the values.
 */
public class HeapMap extends AbstractOsmMap {

    // used on keys
    private static final int  BUCKET_FULL_SHIFT = 63;
    private static final long BUCKET_FULL_MASK  = 1l << BUCKET_FULL_SHIFT;
    private static final long KEY_MASK          = ~BUCKET_FULL_MASK;

    private static final float DEFAULT_FILL_FACTOR       = 0.75f;

    private int capacity;

    private int    size = 0;
    private long[] keys;
    private long[] values;

    private long  hits       = 0;
    private long  misses     = 0;
    private float fillFactor = DEFAULT_FILL_FACTOR;
    private int   threshold;

    class HeapMapError extends Error {
        private static final long serialVersionUID = 1L;

        /**
         * Construct an Error indicating that we've encountered a serious issue
         * 
         * @param message the message
         */
        HeapMapError(@NotNull String message) {
            super(message);
        }
    }

    /**
     * Construct a new map
     * 
     * @param capacity the (fixed) capacity of the map
     */
    public HeapMap(int capacity) {
        this(capacity, DEFAULT_FILL_FACTOR);
    }

    /**
     * Construct a new map
     * 
     * @param capacity the (fixed) capacity of the map
     * @param fill fill factor to use
     */
    public HeapMap(int capacity, float fill) {
        this.capacity = capacity;
        keys = new long[capacity];
        values = new long[capacity];
        fillFactor = fill;
        threshold = (int) (capacity * fillFactor);
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    /**
     * The hash function
     * 
     * @param key the key
     * @return the hash for key
     */
    private static long KEY(long key) {
        return 0x7fffffff & (int) (1664525 * key + 1013904223);
    }

    /*
     * (non-Javadoc)
     * 
     * @see OsmMap#put(long, int, int, int)
     */
    @Override
    public void put(long key, int tileX, int tileY, int neighbours) {
        if (key < 0) {
            throw new IllegalArgumentException("Ids are limited to positive longs");
        }
        int count = 0;

        int bucket = (int) (KEY(key) % capacity);
        long value = createValue(tileX, tileY, neighbours);

        while (true) {
            if (values[bucket] == 0) {
                keys[bucket] = key;
                values[bucket] = value;
                size++;
                return;
            }
            if (size > threshold) {
                throw new HeapMapError("HashMap filled up, increase the (static) capacity!");
            }
            if (count == 0) {
                // mark bucket as "overflow bucket"
                keys[bucket] |= BUCKET_FULL_MASK;
            }
            bucket++;
            count++;
            bucket = bucket % capacity;
        }
    }

    /**
     * Get the bucket index for the value
     * 
     * @param key the key
     * @return the index of -1 if not found
     */
    private int getBucket(long key) {
        int count = 0;
        int bucket = (int) (KEY(key) % capacity);

        while (true) {
            if (values[bucket] != 0l) {
                if ((keys[bucket] & KEY_MASK) == key) {
                    hits++;
                    return bucket;
                }
            } else {
                return -1;
            }

            if (count == 0) {
                // we didn't have an overflow so the value is not stored yet
                if (keys[bucket] >= 0l) {
                    return -1;
                }
            }

            misses++;
            bucket++;
            count++;
            bucket = bucket % capacity;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see OsmMap#get(long)
     */
    @Override
    public long get(long key) {
        int bucket = getBucket(key);

        if (bucket == -1) {
            return 0;
        }

        return values[bucket];
    }

    /*
     * (non-Javadoc)
     * 
     * @see OsmMap#update(long, java.util.List)
     */
    @Override
    public void update(long key, @NotNull Collection<Long> tiles) {

        int bucket = getBucket(key);
        if (bucket == -1) {
            return;
        }

        values[bucket] = updateValue(values[bucket], tiles);

    }

    /*
     * (non-Javadoc)
     * 
     * @see OsmMap#getLoad()
     */
    @Override
    public double getLoad() {
        int count = 0;
        for (int i = 0; i < capacity; i++) {
            if (values[i] != 0) {
                count++;
            }
        }
        return ((double) count) / ((double) capacity);
    }

    /*
     * (non-Javadoc)
     * 
     * @see OsmMap#getMissHitRatio()
     */
    @Override
    public double getMissHitRatio() {
        return ((double) misses) / ((double) hits);
    }

    @Override
    public List<Long> keys() {
        List<Long> result = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            if (values[i] != 0) {
                result.add(keys[i] & KEY_MASK);
            }
        }
        return result;
    }
}
