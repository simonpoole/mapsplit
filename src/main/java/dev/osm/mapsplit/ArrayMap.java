package dev.osm.mapsplit;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * data structure that uses huge arrays with the OSM element's ID (the key) as the array index.
 * 
 * Because the number of IDs can (and in the case of nodes does) exceed Java's maximum array size,
 * additional arrays are used for higher ids.
 */
public final class ArrayMap extends AbstractOsmMap {

    /** maximum size for a single array, see e.g. https://stackoverflow.com/a/8381338/ */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private final long maxKey;

    /** the basic array for IDs starting at 0. This is always used. */ 
    private final @NotNull long[] array0;

    /**
     * additional arrays, only used if the maximum ID is large enough to require it.
     * Not implemented as an array of arrays to avoid the additional indirection.
     */
    private final @Nullable long[] array1, array2, array3, array4, array5;

    /**
     * only keys in range [0, maxKey] will work
     * 
     * @throws IllegalArgumentException  if the requested maximum value is too large
     */
    public ArrayMap(long maxKey) {

        this.maxKey = maxKey;

        long remainingRequiredSize = maxKey + 1;

        this.array0 = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
        remainingRequiredSize -= this.array0.length;

        if (remainingRequiredSize > 0) {
            array1 = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
            remainingRequiredSize -= array1.length;
        } else {
            array1 = null;
        }

        if (remainingRequiredSize > 0) {
            array2 = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
            remainingRequiredSize -= array2.length;
        } else {
            array2 = null;
        }

        if (remainingRequiredSize > 0) {
            array3 = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
            remainingRequiredSize -= array3.length;
        } else {
            array3 = null;
        }

        if (remainingRequiredSize > 0) {
            array4 = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
            remainingRequiredSize -= array4.length;
        } else {
            array4 = null;
        }

        if (remainingRequiredSize > 0) {
            array5 = new long[(int) min(remainingRequiredSize, MAX_ARRAY_SIZE)];
            remainingRequiredSize -= array5.length;
        } else {
            array5 = null;
        }

        if (remainingRequiredSize > 0) {
            throw new IllegalArgumentException("The requested maximum ID is too large for the current implementation");
        }

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
    public final List<Long> keys() {
        List<Long> result = new ArrayList<>();
        for (long key = 0; key <= maxKey; key++) {
            if (get(key) != 0) {
                result.add(key);
            }
        }
        return result;
    }

    /** returns the array containing the value associated with [key] */
    private final long[] arrayForKey(long key) {
        switch((int) (key / MAX_ARRAY_SIZE)) {
        case 0: return array0;
        case 1: return array1;
        case 2: return array2;
        case 3: return array3;
        case 4: return array4;
        case 5: return array5;
        default: throw new IllegalArgumentException("No array for the key " + key + " exists");
        }
    }

    /** returns the index of the value associated with [key], within {@link #arrayForKey(long)} */
    private static final int indexWithinArray(long key) {
        return (int) (key % MAX_ARRAY_SIZE);
    }

}
