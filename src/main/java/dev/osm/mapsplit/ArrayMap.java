package dev.osm.mapsplit;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.LongStream;

import static java.lang.Math.min;

/**
 * data structure that uses huge arrays with the OSM element's ID (the key) as the array index.
 * 
 * Because the number of IDs can (and in the case of nodes does) exceed Java's maximum array size,
 * additional arrays are used for higher ids.
 */
public final class ArrayMap extends AbstractOsmMap {

    /**
     * The maximum size for each array.
     * Must be at most {@link Integer#MAX_VALUE} - 8, see e.g. https://stackoverflow.com/a/8381338/
     */
    private static final int MAX_ARRAY_SIZE = 1 << 30;

    private final long maxKey;

    /**
     * The arrays containing the tile coordinate information for each key.
     * All arrays except the last will have a size equal to {@link #MAX_ARRAY_SIZE}.
     */
    private final long @NotNull[] @NotNull[] arrays;

    /**
     * Creates an empty data structure with a fixed maximum ID.
     *
     * @param maxKey  the largest OSM element ID supported by this instance. Only keys in range [0, maxKey] will work.
     * @throws IllegalArgumentException  if the requested maximum value is too large
     */
    public ArrayMap(long maxKey) {

        this.maxKey = maxKey;

        List<long[]> arrays = new ArrayList<>();

        long remainingRequiredSize = maxKey + 1;

        while (remainingRequiredSize > 0) {
            long[] array = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
            remainingRequiredSize -= array.length;
            arrays.add(array);
        }

        this.arrays = arrays.toArray(new long[0][]);

    }

    @Override
    public final void put(long key, int tileX, int tileY, int neighbours) {
        arrayForKey(key)[indexWithinArray(key)] = createValue(tileX, tileY, neighbours);
    }

    @Override
    public final long get(long key) {
        return arrayForKey(key)[indexWithinArray(key)];
    }

    @Override
    public final void update(long key, Collection<Long> tiles) {
        long[] array = arrayForKey(key);
        int indexWithinArray = indexWithinArray(key);
        array[indexWithinArray] = updateValue(array[indexWithinArray], tiles);
    }

    @Override
    public final double getMissHitRatio() {
        return 0;
    }

    @Override
    public final long getCapacity() {
        return maxKey + 1;
    }

    @Override
    public final LongStream keys() {
        return LongStream.rangeClosed(0, maxKey).filter(key -> get(key) != 0);
    }

    /** returns the array containing the value associated with [key] */
    private final long[] arrayForKey(long key) {
        return arrays[(int) (key / MAX_ARRAY_SIZE)];
    }

    /** returns the index of the value associated with [key], within {@link #arrayForKey(long)} */
    private static final int indexWithinArray(long key) {
        return (int) (key % MAX_ARRAY_SIZE);
    }

}
