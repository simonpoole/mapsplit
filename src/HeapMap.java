/*
 * Mapsplit - A simple but fast tile splitter for large OSM data
 * 
 * Written in 2011 by Peda (osm-mapsplit@won2.de)
 * 
 * To the extent possible under law, the author(s) have dedicated all copyright and 
 * related and neighboring rights to this software to the public domain worldwide. 
 * This software is distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with 
 * this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;


/**
 * An HashMap in Heap memory, optimized for size to use in OSM
 */
@SuppressWarnings("unchecked")
public class HeapMap implements OsmMap {

	// used on keys
	private static final int BUCKET_FULL_SHIFT = 63;
	private static final long BUCKET_FULL_MASK = 1l << BUCKET_FULL_SHIFT;
	private static final long KEY_MASK = ~BUCKET_FULL_MASK;

	// used on values
	static final int TILE_X_SHIFT = 51;
	static final long TILE_X_MASK = 8191l << TILE_X_SHIFT;
	static final int TILE_Y_SHIFT = 38;
	static final long TILE_Y_MASK = 8191l << TILE_Y_SHIFT;
	static final int NEIGHBOUR_SHIFT = 36;
	static final long NEIGHBOUR_MASK = 3l << NEIGHBOUR_SHIFT;
	static final int TILE_EXT_SHIFT = 35;
	static final long TILE_EXT_MASK = 1l << TILE_EXT_SHIFT;		
	static final long TILE_MARKER_MASK = 0x7FFFFFFFFl;

	private int size;
	private long[] keys;
	private long[] values;

	private int extendedBuckets = 0;
	private int[][] extendedSet = new int[1000][];
	
	private long hits = 0;
	private long misses = 0;
	

	public HeapMap(int size) {
		this.size = size;
		keys = new long[size];
		values = new long[size];
	}

	public int tileX(long value) {
		return (int) ((value & TILE_X_MASK) >>> TILE_X_SHIFT);
	}

	public int tileY(long value) {
		return (int) ((value & TILE_Y_MASK) >>> TILE_Y_SHIFT);
	}

	public int neighbour(long value) {
		return (int) ((value & NEIGHBOUR_MASK) >> NEIGHBOUR_SHIFT);
	}

	private static long KEY(long key) {
		return key;
	}
	
	/* (non-Javadoc)
	 * @see OsmMap#put(long, int, int, int)
	 */
	public void put(long key, int tileX, int tileY, int neighbours) {
		int count = 0;

		int bucket = (int) (KEY(key) % size);
		long value = ((long)tileX) << TILE_X_SHIFT | 
					 ((long)tileY) << TILE_Y_SHIFT |
					 ((long) neighbours) << NEIGHBOUR_SHIFT;

		while (true) {
			if (values[bucket] == 0) {
				keys[bucket] = KEY(key);
				values[bucket] = value;
				return;
			} 
			if (count == 0) {
				// mark bucket as "overflow bucket" 
				keys[bucket] |= BUCKET_FULL_MASK;
			}
			bucket++; count++;
			bucket = bucket % size;

			if (count >= size)
				throw new Error("HashMap filled up, increase the (static) size!");
		}
	}

	private int getBucket(long key) {
		int count = 0;
		int bucket = (int) (KEY(key) % size);

		while (true) {
			if (values[bucket] != 0l) {
				if ((keys[bucket] & KEY_MASK) == KEY(key)) {
					hits++;
					return bucket;
				}
			} else {
				return -1;
			}

			if (count == 0) {
				// we didn't have an overflow so the value is not stored yet
				if (keys[bucket] >= 0l)
					return -1;
			}
			
			misses++;
			bucket++; count++;
			bucket = bucket % size;
		}
	}

	/* (non-Javadoc)
	 * @see OsmMap#get(long)
	 */
	public long get(long key) {
		int bucket = getBucket(key);

		if (bucket == -1)
			return 0;

		return values[bucket];
	}

	private int[] merge(int[] old, int[] add, int len) {
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
	
	private void appendNeighbours(int index, Collection<Long> tiles) {
		int[] old = extendedSet[index];
		int[] set = new int[4*tiles.size()];
		int pos = 0;
		
		for (long l : tiles) {
			int tx = tileX(l);
			int ty = tileY(l);
			int neighbour = neighbour(l);

			set[pos++] = tx << 13 | ty;
			if ((neighbour & NEIGHBOURS_EAST) != 0)
				set[pos++] = (tx+1) << 13 | ty;
			if ((neighbour & NEIGHBOURS_SOUTH) != 0)
				set[pos++] = tx << 13 | (ty+1);
			if (neighbour == NEIGHBOURS_SOUTH_EAST)
				set[pos++] = (tx+1) << 13 | (ty+1);
		}
		
		int[] result = merge(old, set, pos);
		extendedSet[index] = result;
	}

	private void extendToNeighbourSet(int bucket, Collection<Long> tiles) {

		long val = values[bucket];

		if ((val & TILE_MARKER_MASK) != 0) {

			// add current stuff to tiles list
			List<Integer> tmpList = parseMarker(val);
			for (int i : tmpList) {
				long tx = i >> 13;
				long ty = i & 8191;
				long temp = tx << TILE_X_SHIFT | ty << TILE_Y_SHIFT;

				tiles.add(temp);
			}

			// delete old marker from val
			val &= ~TILE_MARKER_MASK;
		}

		int cur = extendedBuckets++;

		// if we don't have enough sets, increase the array...
		if (cur >= extendedSet.length) {
			int[][] tmp = new int[2*extendedSet.length][];
			System.arraycopy(extendedSet, 0, tmp, 0, extendedSet.length);
			extendedSet = tmp;
		}

		val |= 1l << TILE_EXT_SHIFT;
		val |= (long) cur;
		values[bucket] = val;

		extendedSet[cur] = new int[0];

		appendNeighbours(cur, tiles);
	}

	/* (non-Javadoc)
	 * @see OsmMap#update(long, java.util.List)
	 */
	public void update(long key, Collection<Long> tiles) {
		
		int bucket = getBucket(key);
		long val = values[bucket];
		int tx = tileX(val);
		int ty = tileY(val);

		// neighbour list is already too large so we use the "large store"
		if ((val & TILE_EXT_MASK) != 0) {

			int idx = (int) (val & TILE_MARKER_MASK);
			appendNeighbours(idx, tiles);

			return;
		}

		// create a expanded temp set for neighbourhood tiles 
		Collection<Long> expanded = new TreeSet<Long>();
		for (long tile : tiles) {
			expanded.add(tile);

			long x = tileX(tile);
			long y = tileY(tile);
			int neighbour = neighbour(tile);
			
			if ((neighbour & NEIGHBOURS_EAST) != 0)
				expanded.add((x+1) << TILE_X_SHIFT | y << TILE_Y_SHIFT);
			if ((neighbour & NEIGHBOURS_SOUTH) != 0)
				expanded.add(x << TILE_X_SHIFT | (y+1) << TILE_Y_SHIFT);
			if (neighbour == NEIGHBOURS_SOUTH_EAST)
				expanded.add((x+1) << TILE_X_SHIFT | (y+1) << TILE_Y_SHIFT);
		}
		
		// now we use the 35 reserved bits for the tiles list..
		boolean extend = false;
		for (long tile : expanded) {

			int tmpX = tileX(tile);
			int tmpY = tileY(tile);

			if (tmpX == tx && tmpY == ty)
				continue;

			int dx = tmpX - tx + 2;
			int dy = tmpY - ty + 2;

			int idx = dy * 6 + dx;
			if (idx >= 14) idx--;

			if (dx < 0 || dy < 0 || dx > 5 || dy > 5) {
				// .. damn, not enough space for "small store" 
				//  -> use "large store" instead
				extend = true;
				break;
			} else {
				val |= 1l << idx;
			}
		}

		if (extend) {
			extendToNeighbourSet(bucket, tiles);
		} else {		
			values[bucket] = val;
		}
	}

	private List<Integer> parseMarker(long value) {
		List<Integer> result = new ArrayList<Integer>();
		int tx = tileX(value);
		int ty = tileY(value);

		for (int i = 0; i < 35; i++) {
			// if bit is not set, continue.. 
			if (((value >> i) & 1) == 0)
				continue;

			int v = i >= 14? i+1 : i;

			int tmpX = v % 6 - 2;
			int tmpY = v / 6 - 2;

			result.add((tx + tmpX) << 13 | (ty + tmpY));
		}
		return result;
	}

	private List<Integer> asList(int[] set) {
		List<Integer> result = new ArrayList<Integer>();
		
		for (int i = 0; i < set.length; i++)
			result.add(set[i]);
		
		return result;
	}
	
	/* (non-Javadoc)
	 * @see OsmMap#getAllTiles(long)
	 */
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

			/* TODO: testen, dieser Teil sollte nicht noetig sein, da schon im extendedSet!
			set.add(tx << 13 | ty);
			if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0)
				set.add(tx+1 << 13 | ty);			
			if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0)
				set.add(tx << 13 | ty+1);

			return set;
			*/
		}

		result = parseMarker(value);

		// TODO: some tiles (neighbour-tiles) might be double-included in the list, is this a problem?!
		
		// add the tile (and possible neighbours)
		result.add(tx << 13 | ty);
		if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0)
			result.add((tx+1) << 13 | ty);			
		if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0)
			result.add(tx << 13 | (ty+1));

		return result;
	}

	/* (non-Javadoc)
	 * @see OsmMap#getLoad()
	 */
	public double getLoad() {
		int count = 0;
		for (int i = 0; i < size; i++) {
			if (values[i] != 0) count++;
		}
		return ((double) count) / ((double) size);
	}
	
	/*
	 * (non-Javadoc)
	 * @see OsmMap#getMissHitRatio()
	 */
	public double getMissHitRatio() {
		return ((double) misses) / ((double) hits);
	}
}
