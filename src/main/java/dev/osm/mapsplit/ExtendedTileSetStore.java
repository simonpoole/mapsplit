package dev.osm.mapsplit;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * an automatically growing collection of extended tile sets. Used by {@link AbstractOsmMap}.
 * This is the solution for elements that belong in a larger number of tiles than what fits in a single long.
 * In addition to automatically resizing the array containing the tile sets (like an {@link ArrayList}),
 * this class also attempts to reduce the amount of duplication by re-using entries for more than one element.
 */
class ExtendedTileSetStore {

    private static final int INITIAL_EXTENDED_SET_SIZE = 1000;

    private int size = 0;
    private int[][] sets = new int[INITIAL_EXTENDED_SET_SIZE][];

    /** returns the set for a given index */
    public int[] get(int index) {
        return sets[index];
    }

    /** adds an extended set and returns the index assigned to it */
    public int add(int[] newSet) {

        if (newSet == null) throw new NullPointerException();

        // check if it equals the most recently added set
        if (size > 0 && Arrays.equals(sets[size - 1], newSet)) {
            return size - 1;
        }

        int index = size++;

        // if we don't have enough sets, increase the array...
        if (index >= sets.length) {
            if (sets.length >= AbstractOsmMap.TILE_MARKER_MASK / 2) { // assumes TILE_MARKER_MASK starts at 0
                throw new IllegalStateException("Too many extended tile entries to expand");
            }
            int[][] tmp = new int[2 * sets.length][];
            System.arraycopy(sets, 0, tmp, 0, sets.length);
            sets = tmp;
        }

        sets[index] = newSet;

        return index;

    }

}
