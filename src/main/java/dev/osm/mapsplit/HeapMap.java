package dev.osm.mapsplit;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.jetbrains.annotations.NotNull;

import it.unimi.dsi.fastutil.longs.LongCollection;

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
    private static long hash(long key) {
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
        long value = createValue(tileX, tileY, neighbours);
        put(key, value);
    }

    /**
     * Put a key and the value in to the map
     * 
     * @param key the key
     * @param value the value
     */
    private void put(long key, long value) {
        if (size > threshold) {
            expand(capacity * 2);
        }
        int bucket = (int) (hash(key) % capacity);
        int count = 0;
        while (true) {
            if (values[bucket] == 0l) {
                keys[bucket] = key;
                values[bucket] = value;
                size++;
                return;
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
     * Expand the map to a larger capacity
     * 
     * @param newCapacity the new capacity
     */
    private void expand(int newCapacity) {
        long[] oldKeys = keys;
        long[] oldValues = values;
        int oldCapacity = capacity;
        capacity = newCapacity;
        keys = new long[capacity];
        values = new long[capacity];
        threshold = (int) (capacity * fillFactor);
        size = 0; // reset
        for (int i = 0; i < oldCapacity; i++) {
            long value = oldValues[i];
            if (value != 0) {
                final long key = oldKeys[i] & ~BUCKET_FULL_MASK;
                put(key, value);
            }
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
        int bucket = (int) (hash(key) % capacity);

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
    public void update(long key, @NotNull LongCollection tiles) {

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
    public LongStream keys() {
        return IntStream.range(0, keys.length)
                .filter(i -> values[i] != 0)
                .mapToLong(i -> keys[i] & KEY_MASK);
    }
}
