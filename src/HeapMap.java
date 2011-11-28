/*
 * OSMSplitter - A simple but fast tile splitter for large OSM data
 * 
 * Written in 2011 by Peda (osm-splitter@won2.de)
 * 
 * To the extent possible under law, the author(s) have dedicated all copyright and 
 * related and neighboring rights to this software to the public domain worldwide. 
 * This software is distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with 
 * this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */

import java.util.ArrayList;
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
	private Set<Integer>[] extendedSet = new Set[1000];


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
	
	/* (non-Javadoc)
	 * @see OsmMap#put(long, int, int, int)
	 */
	public void put(long key, int tileX, int tileY, int neighbours) {
		int count = 0;

		int bucket = (int) (key % size);
		long value = ((long)tileX) << TILE_X_SHIFT | 
		((long)tileY) << TILE_Y_SHIFT |
		((long) neighbours) << NEIGHBOUR_SHIFT;

		while (true) {
			if (values[bucket] == 0) {
				keys[bucket] = key;
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

		int bucket = (int) (key % size);

		while (true) {
			if ((values[bucket] != 0) && 
					((keys[bucket] & KEY_MASK) == key)) {
				return bucket;
			}
			if (count == 0) {
				// we didn't have an overflow so the value is not stored yet
				if (keys[bucket] >= 0)
					return -1;
			}
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

	private void appendNeighbours(int index, List<Long> tiles) {
		Set<Integer> set = extendedSet[index];

		for (long l : tiles) {
			int tx = tileX(l);
			int ty = tileY(l);

			set.add(tx << 13 | ty);
		}
	}

	private void extendToNeighbourSet(int bucket, List<Long> tiles) {

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
			Set<Integer>[] tmp = new Set[2*extendedSet.length];
			System.arraycopy(extendedSet, 0, tmp, 0, extendedSet.length);
			extendedSet = tmp;
		}

		val |= 1l << TILE_EXT_SHIFT;
		val |= (long) cur;
		values[bucket] = val;

		extendedSet[cur] = new TreeSet<Integer>();

		appendNeighbours(cur, tiles);
	}

	/* (non-Javadoc)
	 * @see OsmMap#update(long, java.util.List)
	 */
	public void update(long key, List<Long> tiles) {

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

		// now we use the 35 reserved bits for the tiles list..
		boolean extend = false;
		for (long tile : tiles) {

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
			Set<Integer> set = extendedSet[idx];

			result = new ArrayList<Integer>(set);
			result.add(tx << 13 | ty);

			// TODO neighbourhood treatment...

			return result;				
		}

		result = parseMarker(value);

		result.add(tx << 13 | ty);
		// TODO neighbourhood treatment...

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
}
