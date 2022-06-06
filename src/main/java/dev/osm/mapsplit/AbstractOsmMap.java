package dev.osm.mapsplit;

import java.util.Collection;

import it.unimi.dsi.fastutil.ints.*;
import org.jetbrains.annotations.NotNull;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongCollection;

/*-
 * provides implementation aspects shared between multiple {@link OsmMap} subtypes,
 * particularly relating to the interpretation of the map's values.
 * 
 * Structure of the values:
 *
 *     6                   4                   332  2
 *     3                   7                   109  7
 *     XXXX XXXX XXXX XXXX YYYY YYYY YYYY YYYY 1ENN xxxx nnnn nnnn nnnn nnnn nnnn nnnn
 *
 *     X - tile number
 *     Y - tile number
 *     1 - always set to 1. This ensures that the value can be distinguished from empty positions in an array.
 *     N - bits indicating immediate "neigbours"
 *     E - extended "neighbour" list used
 *     x - additional bits used together with NN in extended list mode
 *     n - bits for "short" neighbour index, in extended list mode used as index
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
 */
public abstract class AbstractOsmMap implements OsmMap {

    // see HEAPMAP.md for details
    static final int          TILE_X_SHIFT = 48;
    static final int          TILE_Y_SHIFT = 32;
    private static final long TILE_X_MASK  = Const.MAX_TILE_NUMBER << TILE_X_SHIFT;
    private static final long TILE_Y_MASK  = Const.MAX_TILE_NUMBER << TILE_Y_SHIFT;

    private static final int  TILE_EXT_SHIFT            = 30;
    private static final long TILE_EXT_MASK             = 1l << TILE_EXT_SHIFT;
    static final long         TILE_MARKER_MASK          = 0xFFFFFFl;
    private static final long TILE_EXT_INDEX_MASK       = 0x3FFFFFFFl;
    private static final int  NEIGHBOUR_SHIFT           = TILE_EXT_SHIFT - 2;
    private static final long NEIGHBOUR_MASK            = 3l << NEIGHBOUR_SHIFT;
    private static final int  ONE_BIT_SHIFT             = 31;
    private static final long ONE_BIT_MASK              = 1l << ONE_BIT_SHIFT;

    private final ExtendedTileSetStore extendedSets = new ExtendedTileSetStore();

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
    public IntSet getAllTiles(long key) {

        long value = get(key);

        if (value == 0) {
            return null;
        } else if ((value & TILE_EXT_MASK) != 0) {
            int idx = (int) (value & TILE_EXT_INDEX_MASK);
            return new IntArraySet(extendedSets.get(idx));
        } else {
            IntSet result = parseNeighbourBits(value);
            result.add(TileCoord.encode(tileX(value), tileY(value)));
            return result;
        }

    }

    @Override
    public void updateInt(long key, IntCollection tiles) {
        LongArrayList longTiles = new LongArrayList();
        for (int tile : tiles) {
            longTiles.add(createValue(TileCoord.decodeX(tile), TileCoord.decodeY(tile), NEIGHBOURS_NONE));
        }
        this.update(key, longTiles);
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
        return ((long) tileX) << TILE_X_SHIFT | ((long) tileY) << TILE_Y_SHIFT | ((long) neighbours) << NEIGHBOUR_SHIFT | ONE_BIT_MASK;
    }

    /**
     * updates a value by adding a set of tiles to it. Might "overflow" into the list of additional tiles and modify it
     * accordingly.
     * 
     * This can be used to implement {@link #update(long, LongCollection)}.
     * 
     * @param originalValue the original value
     * @param tiles a collection of tiles to add
     * @return the updated value
     */
    protected long updateValue(long originalValue, @NotNull LongCollection tiles) {

        long val = originalValue;

        int tx = tileX(val);
        int ty = tileY(val);

        // neighbour list is already too large so we use the "large store"
        if ((val & TILE_EXT_MASK) != 0) {
            int idx = (int) (val & TILE_EXT_INDEX_MASK);
            IntSet set = new IntOpenHashSet(extendedSets.get(idx));
            tiles.forEach(tile -> set.addAll(decode(tile)));
            int newSetIndex = extendedSets.add(set.toIntArray());
            val |= TILE_EXT_MASK;
            val &= ~TILE_EXT_INDEX_MASK; // overwrite any existing content with 0s
            val |= newSetIndex;
            return val;
        }

        // create a expanded temp set for neighbourhood tiles
        IntSet expanded = new IntOpenHashSet();
        for (long tile : tiles) {
            final int t = (int) (tile >>> TILE_Y_SHIFT);
            expanded.add(t);
            int ttx = TileCoord.decodeX(t);
            int tty = TileCoord.decodeY(t);

            parseNnBits(expanded, neighbour(tile), ttx, tty);
        }

        // now we use the 24 reserved bits for the tiles list..
        for (int tile : expanded) {

            int tmpX = TileCoord.decodeX(tile);
            int tmpY = TileCoord.decodeY(tile);

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
                return extendToNeighbourSet(originalValue, tiles);
            } else {
                val |= 1l << idx;
            }
        }
        return val;
    }

    /**
     * Check the NN bits and add tiles to result
     * 
     * @param result the IntSet holding the result
     * @param neighbour the immediate neighbours
     * @param tx tile x
     * @param ty tile y
     */
    public void parseNnBits(IntSet result, int neighbour, int tx, int ty) {
        // add possible neighbours
        if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0) {
            result.add(TileCoord.encode(tx + 1, ty));
        }
        if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0) {
            result.add(TileCoord.encode(tx, ty + 1));
        }
        if ((neighbour & OsmMap.NEIGHBOURS_SOUTH_EAST) == OsmMap.NEIGHBOURS_SOUTH_EAST) {
            result.add(TileCoord.encode(tx + 1, ty + 1));
        }
    }

    /**
     * Start using an index for an {@link ExtendedTileSetStore}
     * instead of the bits directly in the value
     * 
     * @param val the value to extend
     * @param tiles the List of additional tiles
     * @return the updated value
     */
    private long extendToNeighbourSet(long val, @NotNull Collection<Long> tiles) {

        IntSet tileSet = parseNeighbourBits(val);
        tileSet.add(TileCoord.encode(tileX(val), tileY(val)));
        tiles.forEach(tile -> tileSet.addAll(decode(tile)));
        int extendedSetIndex = extendedSets.add(tileSet.toIntArray());

        val |= TILE_EXT_MASK;
        val &= ~TILE_EXT_INDEX_MASK; // overwrite any existing content with 0s
        val |= extendedSetIndex;

        return val;
    }

    /** transforms a map value into a list of integer tile coords (using {@link TileCoord} encoding) */
    private IntSet decode(long value) {

        IntSet result = new IntOpenHashSet();

        int tx = tileX(value);
        int ty = tileY(value);
        result.add(TileCoord.encode(tx, ty));
        parseNnBits(result, neighbour(value), tx, ty);

        return result;

    }

    /**
     * Get neighboring tiles from bit values
     * 
     * @param value the encoded value
     * @return a list of tiles encoded in an int
     */
    private IntSet parseNeighbourBits(long value) {
        IntSet result = new IntArraySet();

        int tx = tileX(value);
        int ty = tileY(value);

        parseNnBits(result, neighbour(value), tx, ty);

        if ((value & TILE_MARKER_MASK) != 0) {
            for (int i = 0; i < 24; i++) {
                // if bit is not set, continue..
                if (((value >> i) & 1) == 0) {
                    continue;
                }

                int v = i >= 12 ? i + 1 : i;

                int tmpX = v % 5 - 2;
                int tmpY = v / 5 - 2;

                result.add(TileCoord.encode(tx + tmpX, ty + tmpY));
            }
        }
        return result;
    }

}
