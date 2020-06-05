package dev.osm.mapsplit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.jetbrains.annotations.NotNull;

//@formatter:off
/**
 * provides implementation aspects shared between multiple {@link OsmMap} subtypes,
 * particularly relating to the interpretation of the map's values.
 * 
 * Structure of the values:
 *
 *     6                 4                   3    2 2 22
 *     3                 8                   2    8 7 54
 *     XXXX XXXX XXXX XXXX YYYY YYYY YYYY YYYY 1uuu uNNE nnnn nnnn nnnn nnnn nnnn nnnn
 *
 *     X - tile number
 *     Y - tile number
 *     u - unused
 *     1 - always set to 1. This ensures that the value can be distinguished from empty positions in an array.
 *     N - bits indicating immediate "neigbours"
 *     E - extended "neighbour" list used
 *     n - bits for "short" neighbour index, in long list mode used as index
 *
 *     Tiles indexed in "short" list (T original tile)
 *           -  - 
 *           2  1  0  1  2
 *       
 *     -2    0  1  2  3  4
 *     -1    5  6  7  8  9
 *      0   10 11  T 12 13
 *      1   14 15 16 17 18
 *      2   19 20 21 22 23
 * 
 */
//@formatter:on
abstract public class AbstractOsmMap implements OsmMap {

    // see HEAPMAP.md for details
    static final int          TILE_X_SHIFT = 48;
    static final int          TILE_Y_SHIFT = 32;
    private static final long TILE_X_MASK  = Const.MAX_TILE_NUMBER << TILE_X_SHIFT;
    private static final long TILE_Y_MASK  = Const.MAX_TILE_NUMBER << TILE_Y_SHIFT;

    private static final int   TILE_EXT_SHIFT            = 24;
    private static final long  TILE_EXT_MASK             = 1l << TILE_EXT_SHIFT;
    private static final long  TILE_MARKER_MASK          = 0xFFFFFFl;
    private static final int   NEIGHBOUR_SHIFT           = TILE_EXT_SHIFT + 1;
    private static final long  NEIGHBOUR_MASK            = 3l << NEIGHBOUR_SHIFT;
    private static final int   ONE_BIT_SHIFT             = 31;
    private static final long  ONE_BIT_MASK              = 1l << ONE_BIT_SHIFT;
    private static final int   INITIAL_EXTENDED_SET_SIZE = 1000;

    private int     extendedBuckets = 0;
    private int[][] extendedSet     = new int[INITIAL_EXTENDED_SET_SIZE][];

    @Override
    public int tileX(long value) {
        return (int) ((value & TILE_X_MASK) >>> TILE_X_SHIFT);
    }

    @Override
    public int tileY(long value) {
        return (int) ((value & TILE_Y_MASK) >>> TILE_Y_SHIFT);
    }

    @Override
    public int neighbour(long value) {
        return (int) ((value & NEIGHBOUR_MASK) >> NEIGHBOUR_SHIFT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see OsmMap#getAllTiles(long)
     */
    @Override
    public List<Integer> getAllTiles(long key) {

        List<Integer> result;
        long value = get(key);

        if (value == 0) {
            return null;
        }

        int tx = tileX(value);
        int ty = tileY(value);
        int neighbour = neighbour(value);

        if ((value & TILE_EXT_MASK) != 0) {
            int idx = (int) (value & TILE_MARKER_MASK);
            return asList(extendedSet[idx]);

            /*
             * TODO: testen, dieser Teil sollte nicht noetig sein, da schon im extendedSet! set.add(tx << 13 | ty); if
             * ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0) set.add(tx+1 << 13 | ty); if ((neighbour &
             * OsmMap.NEIGHBOURS_SOUTH) != 0) set.add(tx << 13 | ty+1);
             * 
             * return set;
             */
        }

        result = parseMarker(value);

        // TODO: some tiles (neighbour-tiles) might be double-included in the list, is this a problem?!

        // add the tile (and possible neighbours)
        result.add(tx << Const.MAX_ZOOM | ty);
        if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0) {
            result.add((tx + 1) << Const.MAX_ZOOM | ty);
        }
        if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0) {
            result.add(tx << Const.MAX_ZOOM | (ty + 1));
        }

        return result;
    }

    /**
     * creates a value (representing a set of tile coords) from a pair of x/y tile coords and maybe some neighbors
     * 
     * @param tileX x tile number
     * @param tileY y tile number
     * @param neighbours bit encoded neighbours
     * @return the value encoding the set of tiles represented by the parameters
     */
    protected long createValue(int tileX, int tileY, int neighbours) {
        return ((long) tileX) << TILE_X_SHIFT | ((long) tileY) << TILE_Y_SHIFT
                | ((long) neighbours) << NEIGHBOUR_SHIFT
                | ONE_BIT_MASK;
    }

    /**
     * updates a value by adding a set of tiles to it.
     * Might "overflow" into the list of additional tiles and modify it accordingly.
     * 
     * This can be used to implement {@link #update(long, Collection)}.
     * 
     * @return  the updated value
     */
    protected long updateValue(long originalValue, @NotNull Collection<Long> tiles) {

        long val = originalValue;

        int tx = tileX(val);
        int ty = tileY(val);

        // neighbour list is already too large so we use the "large store"
        if ((val & TILE_EXT_MASK) != 0) {
            int idx = (int) (val & TILE_MARKER_MASK);
            appendNeighbours(idx, tiles);
            return originalValue;
        }

        // create a expanded temp set for neighbourhood tiles
        Collection<Long> expanded = new TreeSet<>();
        for (long tile : tiles) {
            expanded.add(tile);

            long x = tileX(tile);
            long y = tileY(tile);
            int neighbour = neighbour(tile);

            if ((neighbour & NEIGHBOURS_EAST) != 0) {
                expanded.add((x + 1) << TILE_X_SHIFT | y << TILE_Y_SHIFT);
            }
            if ((neighbour & NEIGHBOURS_SOUTH) != 0) {
                expanded.add(x << TILE_X_SHIFT | (y + 1) << TILE_Y_SHIFT);
            }
            if (neighbour == NEIGHBOURS_SOUTH_EAST) {
                expanded.add((x + 1) << TILE_X_SHIFT | (y + 1) << TILE_Y_SHIFT);
            }
        }

        // now we use the 24 reserved bits for the tiles list..
        boolean extend = false;
        for (long tile : expanded) {

            int tmpX = tileX(tile);
            int tmpY = tileY(tile);

            if (tmpX == tx && tmpY == ty) {
                continue;
            }

            int dx = tmpX - tx + 2;
            int dy = tmpY - ty + 2;

            int idx = dy * 5 + dx;
            if (idx >= 12) {
                idx--;
            }

            if (dx < 0 || dy < 0 || dx > 4 || dy > 4) {
                // .. damn, not enough space for "small store"
                // -> use "large store" instead
                extend = true;
                break;
            } else {
                val |= 1l << idx;
            }
        }

        if (extend) {
            return extendToNeighbourSet(originalValue, tiles);
        } else {
            return val;
        }

    }

    /**
     * Start using the list of additional tiles instead of the bits directly in the value
     * 
     * @param val the value to extend
     * @param tiles the List of additional tiles
     * @return the updated value
     */
    private long extendToNeighbourSet(long val, @NotNull Collection<Long> tiles) {

        if ((val & TILE_MARKER_MASK) != 0) {
            // add current stuff to tiles list
            List<Integer> tmpList = parseMarker(val);
            for (int i : tmpList) {
                long tx = i >>> Const.MAX_ZOOM;
                long ty = i & Const.MAX_TILE_NUMBER;
                long temp = tx << TILE_X_SHIFT | ty << TILE_Y_SHIFT;

                tiles.add(temp);
            }

            // delete old marker from val
            val &= ~TILE_MARKER_MASK;
        }

        int cur = extendedBuckets++;

        // if we don't have enough sets, increase the array...
        if (cur >= extendedSet.length) {
            if (extendedSet.length >= TILE_MARKER_MASK / 2) { // assumes TILE_MARKER_MASK starts at 0
                throw new IllegalStateException("Too many extended tile entries to expand");
            }
            int[][] tmp = new int[2 * extendedSet.length][];
            System.arraycopy(extendedSet, 0, tmp, 0, extendedSet.length);
            extendedSet = tmp;
        }

        val |= 1l << TILE_EXT_SHIFT;
        val |= (long) cur;

        extendedSet[cur] = new int[0];

        appendNeighbours(cur, tiles);

        return val;
    }

    /**
     * Add tiles to the extended tile list for an element
     * 
     * @param index the index in to the array of the tile lists
     * @param tiles a List containing the tile numbers
     */
    private void appendNeighbours(int index, @NotNull Collection<Long> tiles) {
        int[] old = extendedSet[index];
        int[] set = new int[4 * tiles.size()];
        int pos = 0;

        for (long l : tiles) {
            int tx = tileX(l);
            int ty = tileY(l);
            int neighbour = neighbour(l);

            set[pos++] = tx << Const.MAX_ZOOM | ty;
            if ((neighbour & NEIGHBOURS_EAST) != 0) {
                set[pos++] = (tx + 1) << Const.MAX_ZOOM | ty;
            }
            if ((neighbour & NEIGHBOURS_SOUTH) != 0) {
                set[pos++] = tx << Const.MAX_ZOOM | (ty + 1);
            }
            if (neighbour == NEIGHBOURS_SOUTH_EAST) {
                set[pos++] = (tx + 1) << Const.MAX_ZOOM | (ty + 1);
            }
        }

        int[] result = merge(old, set, pos);
        extendedSet[index] = result;
    }

    private List<Integer> parseMarker(long value) {
        List<Integer> result = new ArrayList<>();
        int tx = tileX(value);
        int ty = tileY(value);

        for (int i = 0; i < 24; i++) {
            // if bit is not set, continue..
            if (((value >> i) & 1) == 0) {
                continue;
            }

            int v = i >= 12 ? i + 1 : i;

            int tmpX = v % 5 - 2;
            int tmpY = v / 5 - 2;

            result.add((tx + tmpX) << Const.MAX_ZOOM | (ty + tmpY));
        }
        return result;
    }

    /**
     * Merge two int arrays, removing dupes
     * 
     * @param old the original array
     * @param add the additional array
     * @param len the additional number of ints to allocate
     * @return the new array
     */
    private static int[] merge(@NotNull int[] old, @NotNull int[] add, int len) {
        int curLen = old.length;
        int[] tmp = new int[curLen + len];
        System.arraycopy(old, 0, tmp, 0, old.length);

        for (int i = 0; i < len; i++) {
            int toAdd = add[i];
            boolean contained = false;

            for (int j = 0; j < curLen; j++) {
                if (toAdd == tmp[j]) {
                    contained = true;
                    break;
                }
            }

            if (!contained) {
                tmp[curLen++] = toAdd;
            }
        }

        int[] result = new int[curLen];
        System.arraycopy(tmp, 0, result, 0, curLen);

        return result;
    }

    /**
     * Return a list with the contents of an int array
     * 
     * @param set the array
     * @return a List of Integer
     */
    @NotNull
    private static List<Integer> asList(@NotNull int[] set) {
        List<Integer> result = new ArrayList<>();

        for (int i = 0; i < set.length; i++) {
            result.add(set[i]);
        }

        return result;
    }

}
