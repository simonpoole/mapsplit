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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.openstreetmap.osmosis.core.container.v0_6.BoundContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Bound;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableSource;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.file.BlockOutputStream;
import crosby.binary.osmosis.OsmosisReader;
import crosby.binary.osmosis.OsmosisSerializer;


public class MapSplit {

	/*
	 * the zoom-level at which we render our tiles
	 * Attention: Code is not generic enough to chance this value without
	 * further code changes! ;)
	 */	
	private static final int ZOOM = 13;
	
	/*
	 * the default sizes for the hash maps: should be a factor 2-4 of nodes 
	 * in the pbf you want to read
	 */  
	private static final int NODE_MAP_SIZE     = 60000000;
	private static final int WAY_MAP_SIZE      = 10000000;	
	private static final int RELATION_MAP_SIZE =  2500000;
	
	// all data after this appointment date is considered new or modified
	private Date appointmentDate;
	
	private Date latestDate = new Date(0);

	// the size of the border (in percent for a tile's height and width) for single tiles
	private double border = 0.1;
	
	// the input file we're going to split
	private File input;
	
	// maximum number of files open at the same time
	private int maxFiles;
	
	// internal store to check if reading the file worked
	private boolean complete = false;

	// the hashmap for all nodes in the osm map
	private OsmMap nmap;

	// the hashmap for all ways in the osm map
	private OsmMap wmap;

	// the hashmap for all relations in the osm map
	private OsmMap rmap;

	// a map of ways that need to be added in a second run
	private HashMap<Long, Collection<Long>> extraWayMap = null;
	
	// a bitset telling the algorithm which tiles need to be rerendered
	private BitSet modifiedTiles = new BitSet();
	
	// the serializer (OSM writers) for any modified tile
	private Map<Integer, OsmosisSerializer> outFiles;
	

	
	
	public MapSplit(Date appointmentDate, int[] mapSizes, int maxFiles, double border, File inputFile, boolean completeRelations) {
		this.border = border;
		this.input = inputFile;
		this.appointmentDate = appointmentDate;
		this.maxFiles = maxFiles;
		nmap = new HeapMap(mapSizes[0]);
		wmap = new HeapMap(mapSizes[1]);
		rmap = new HeapMap(mapSizes[2]);
		if (completeRelations)
			extraWayMap = new HashMap<Long, Collection<Long>>();
	}

	public static double tile2lon(int x) {
		return (x / Math.pow(2.0, ZOOM)) * 360.0 - 180.0;
	}
	
	public static double tile2lat(int y) {
		double n = Math.PI - 2.0 * Math.PI * y / Math.pow(2, ZOOM);
	    return (180.0 / Math.PI * Math.atan(0.5 * (Math.pow(Math.E, n) - Math.pow(Math.E, -n))));
	}
	
	public static int lon2tileX(double lon) {
		return (int) Math.floor((lon + 180.0) / 360.0 * Math.pow(2.0,ZOOM));
	}
	
	public static int lat2tileY(double lat) { 
		return (int) Math.floor((1.0 - Math.log(Math.tan(lat * Math.PI / 180.0) + 1.0/Math.cos(lat * Math.PI / 180.0)) / Math.PI) / 2.0 * Math.pow(2.0,ZOOM));
	}
	
	/* Calculate the Bound for the given tile */
	public Bound getBound(int tileX, int tileY) {
		
		double l = tile2lon(tileX);
		double r = tile2lon(tileX+1);
		double t = tile2lat(tileY);
		double b = tile2lat(tileY+1);
		
		double dx = r - l;
		double dy = b - t;
		
		l -= border * dx;
		r += border * dx;
		t -= border * dy;
		b += border * dy;
		
		return new Bound(r, l, t, b, "mapsplit");
	}

	private void checkAndFill(Collection<Long> tiles) {
		int minX = Integer.MAX_VALUE, minY = Integer.MAX_VALUE;
		int maxX = Integer.MIN_VALUE, maxY = Integer.MIN_VALUE;
		
		// determine the min/max tile nrs
		for (long tile : tiles) {
			int tx = nmap.tileX(tile);
			int ty = nmap.tileY(tile);
			
			minX = Math.min(minX, tx);
			minY = Math.min(minY, ty);
			maxX = Math.max(maxX, tx);
			maxY = Math.max(maxY, ty);
		}
		
		// enlarge min/max to have a border and to cope with possible neighbourhood tiles
		minX -= 2; minY -= 2; maxX += 2; maxY += 2;
		int sizeX = maxX - minX + 1;
		int sizeY = maxY - minY + 1;
		
		// fill the helperSet which marks any set tile
		BitSet helperSet = new BitSet();
		for (long tile : tiles) {
			int tx = nmap.tileX(tile) - minX;
			int ty = nmap.tileY(tile) - minY;
			int neighbour = nmap.neighbour(tile);
			
			helperSet.set(tx + ty * sizeX);
			if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0)
				helperSet.set(tx+1 + ty * sizeX);
			if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0)
				helperSet.set(tx + (ty+1) * sizeX);
			if (neighbour == OsmMap.NEIGHBOURS_SOUTH_EAST)
				helperSet.set(tx+1 + (ty+1) * sizeX);
		}
		
		// start with tile 1,1 and fill region...
		Stack<Integer> stack = new Stack<Integer>();
		stack.push(1 + 1 * sizeX);
	
		// fill all tiles that are reachable by a 4-neighbourhood
		while (!stack.isEmpty()) {
			int val = stack.pop();
			
			boolean isSet = helperSet.get(val);
			helperSet.set(val);
			
			if (val >= sizeX * sizeY)
				continue;
			
			int ty = val / sizeX;
			int tx = val % sizeX;
			
			if ((tx == 0) || (ty == 0) || (ty >= sizeY))
				continue;
			
			if (!isSet) {
				stack.push(tx+1 + ty * sizeX);
				stack.push(tx-1 + ty * sizeX);
				stack.push(tx + (ty+1) * sizeX);
				stack.push(tx + (ty-1) * sizeX);
			}
		}
		
		// now check if there are not-set bits left (i.e. holes in tiles)
		int idx = -1;
		while (true) {
			idx = helperSet.nextClearBit(idx+1);
			
			if (idx >= sizeX * sizeY)
				break;
			
			int tx = idx % sizeX;
			int ty = idx / sizeX;
			
			if ((tx == 0) || (ty == 0))
				continue;
			
			tx += minX;
			ty += minY;
			
			// TODO: make this a bit nicer by delegating the id-generation to the map code
			tiles.add(((long) tx) << 51 | ((long) ty) << 38);
			modifiedTiles.set(tx << 13 | ty);
		}
	}
	
	/* calculate the lon-offset for the given border size */
	private double deltaX(double lon) {
		int tx = lon2tileX(lon);
		
		double x1 = tile2lon(tx);
		double x2 = tile2lon(tx+1);
		
		return border * (x2 - x1);
	}
	
	/* calculate the lat-offset for the given border size */
	private double deltaY(double lat) {
		int ty = lat2tileY(lat);
		
		double y1 = tile2lat(ty);
		double y2 = tile2lat(ty+1);
		
		return border * (y2 - y1);
	}

	private void addNodeToMap(Node n, double lat, double lon) {
		int tileX = lon2tileX(lon);
		int tileY = lat2tileY(lat);
		int neighbour = OsmMap.NEIGHBOURS_NONE;
		
		// check and add border if needed
		double dx = deltaX(lon);
		if (lon2tileX(lon+dx) > tileX) {
			neighbour = OsmMap.NEIGHBOURS_EAST;
		} else if (lon2tileX(lon-dx) < tileX) {
			tileX--;
			neighbour = OsmMap.NEIGHBOURS_EAST;
		}
		double dy = deltaY(lat);
		if (lat2tileY(lat+dy) > tileY) {
			neighbour += OsmMap.NEIGHBOURS_SOUTH;
		} else if (lat2tileY(lat-dy) < tileY) {
			tileY--;
			neighbour += OsmMap.NEIGHBOURS_SOUTH;
		}
		
		// mark current tile (and neighbours) to be rerendered
		if (n.getTimestamp().after(appointmentDate)) {
			modifiedTiles.set(tileX << 13 | tileY);
			if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0)
				modifiedTiles.set((tileX+1) << 13 | tileY);			
			if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0)
				modifiedTiles.set(tileX << 13 | (tileY+1));
			if (neighbour == OsmMap.NEIGHBOURS_SOUTH_EAST)
				modifiedTiles.set((tileX+1) << 13 | (tileY+1));
		}
		
		// mark the latest changes made to this map
		if (n.getTimestamp().after(latestDate))
			latestDate = n.getTimestamp();
		
		nmap.put(n.getId(), tileX, tileY, neighbour);
	}
	
	private void addWayToMap(Way way) {
		
		boolean modified = way.getTimestamp().after(appointmentDate);
		Set<Long> tileList = new TreeSet<Long>();
		
		// mark the latest changes made to this map
		if (way.getTimestamp().after(latestDate))
			latestDate = way.getTimestamp();
		
		for (WayNode wayNode : way.getWayNodes()) {
			// get tileNrs for given node
			long tile = nmap.get(wayNode.getNodeId());

			// ignore missing nodes (extract with incomplete ways)
			if (tile == 0)
				continue;

			// mark tiles (and possible neighbours) as modified
			if (modified) {
				int tx = nmap.tileX(tile);
				int ty = nmap.tileY(tile);
				int neighbour = nmap.neighbour(tile);
				
				modifiedTiles.set(tx << 13 | ty);
				if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0)
					modifiedTiles.set((tx+1) << 13 | ty);			
				if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0)
					modifiedTiles.set(tx << 13 | (ty+1));
				if (neighbour == OsmMap.NEIGHBOURS_SOUTH_EAST)
					modifiedTiles.set((tx+1) << 13 | (ty+1));
			}
			
			tileList.add(tile);
		}

		// TODO check/verify if 8 tiles is ok or if there might be corner-cases with only 4 tiles
		// with more than 8 (or 4?!) tiles in the list we might have a "hole"
		if (tileList.size() >= 8) {
			checkAndFill(tileList);
		}
			
		// bootstrap a tilepos for the way
		long id = way.getWayNodes().get(0).getNodeId();
		long val = nmap.get(id);
		int tx = nmap.tileX(val);
		int ty = nmap.tileY(val);
		
		// put way into map with a "random" base tile
		wmap.put(way.getId(), tx, ty, OsmMap.NEIGHBOURS_NONE);
		// update map so that the way knows which tiles it belongs to
		wmap.update(way.getId(), tileList);
		
		for (WayNode wayNode : way.getWayNodes()) {
			
			// update map so that the node knows about any additional
			// tile it has to be stored in
			nmap.update(wayNode.getNodeId(), tileList);
		}
	}
	
	private void addExtraWayToMap(Way way, Collection<Long> tileList) {
		
		for (WayNode wayNode : way.getWayNodes()) {

			// update map so that the node knows about any additional
			// tile it has to be stored in
			nmap.update(wayNode.getNodeId(), tileList);
		}
	}
	
	private void addRelationToMap(Relation r) {
		
		boolean multipolygon = false;
		boolean boundary = false;
		
		for (Tag tag : r.getTags()) {	
			if (tag.getKey().equals("type") && tag.getValue().equals("multipolygon"))
				multipolygon = true;
			if (tag.getKey().equals("boundary"))
				boundary = true;
		}
		
		// We only treat multipolygons right now
		if (!multipolygon || boundary)
			return;
		
		boolean modified = r.getTimestamp().after(appointmentDate);
		Collection<Long> tileList = new TreeSet<Long>();
		
		if (r.getTimestamp().after(latestDate))
			latestDate = r.getTimestamp();
		
		for (RelationMember m : r.getMembers()) {
		
			switch (m.getMemberType()) {
			case Node:
				long tile = nmap.get(m.getMemberId());
				
				// The referenced node is not in our data set
				if (tile == 0) {
					System.out.println("Non-complete Relation " + r.getId() + " (missing a node)");
					continue;
				}
				
				// mark tiles as modified
				if (modified) {
					int tx = nmap.tileX(tile);
					int ty = nmap.tileY(tile);
					int neighbour = nmap.neighbour(tile);

					modifiedTiles.set(tx << 13 | ty);
					if ((neighbour & OsmMap.NEIGHBOURS_EAST) != 0)
						modifiedTiles.set((tx+1) << 13 | ty);			
					if ((neighbour & OsmMap.NEIGHBOURS_SOUTH) != 0)
						modifiedTiles.set(tx << 13 | (ty+1));
					if (neighbour == OsmMap.NEIGHBOURS_SOUTH_EAST)
						modifiedTiles.set((tx+1) << 13 | (ty+1));
				}
				
				tileList.add(tile);
				break;
				
			case Way:
				List<Integer> list = wmap.getAllTiles(m.getMemberId());
				
				// The referenced way is not in our data set
				if (list == null) {
					System.out.println("Non-complete Relation " + r.getId() + " (missing a way)");
					return;
				}
				
				if (modified) {
					for (Integer i : list)
						modifiedTiles.set(i);
				}

				// TODO: make this a bit more generic / nicer code :/
				for (int i : list)
					tileList.add(((long) i) << 38);
				break;
				
			case Relation:
			default:
				// Not handled
			}
		}
		
		// Just in case, this can happen due to silly input data :'(
		if (tileList.isEmpty()) {
			System.out.println("Ignoring empty relation");
			return;
		}
		
		if (tileList.size() >= 8) {
			checkAndFill(tileList);
		}
		
		long val = tileList.iterator().next();
		int tx = rmap.tileX(val);
		int ty = rmap.tileY(val);
			
		// put relation into map with a "random" base tile
		rmap.put(r.getId(), tx, ty, OsmMap.NEIGHBOURS_NONE);
		// update map so that the relation knows in which tiles it is needed
		rmap.update(r.getId(), tileList);
		
		for (RelationMember m : r.getMembers()) {
			switch (m.getMemberType()) {
			case Node:
				nmap.update(m.getMemberId(), tileList);
				break;
			case Way:
				wmap.update(m.getMemberId(), tileList);
				if (extraWayMap != null)
					extraWayMap.put(m.getMemberId(), tileList);
				break;
			case Relation:
			default:
				// not handled
			}		
		}
	}
	
	public void setup() throws IOException {
		
		RunnableSource reader =	new OsmosisReader(new FileInputStream(input));
		reader.setSink(new Sink() {
			@Override
			public void release() { /* nothing to be done */ }			
			@Override
			public void complete() { complete = true; }
			@Override
			public void process(EntityContainer ec) {
				if (ec instanceof NodeContainer) {
					
					Node n = ((NodeContainer) ec).getEntity();
					addNodeToMap(n, n.getLatitude(), n.getLongitude());
					
				} else if (ec instanceof WayContainer) {
					
					Way w = ((WayContainer) ec).getEntity();
					addWayToMap(w);
					
				} else if (ec instanceof RelationContainer) {
					
					Relation r = ((RelationContainer) ec).getEntity();
					addRelationToMap(r);
					
				} else if (ec instanceof BoundContainer) {
					
					// nothing todo, we ignore bound tags
					
				} else {
					System.err.println("Unknown Element while reading");
					System.err.println(ec.toString());
					System.err.println(ec.getEntity().toString());
				}
			}
		});
		
		Thread readerThread = new Thread(reader);
		readerThread.start();		
		while (readerThread.isAlive()) {
			try {
				readerThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if (!complete)
			throw new IOException("Could not read file fully");
		
		
		// Second run if we are in complete-relation-mode
		if (extraWayMap != null) {

			complete = false;
			
			reader = new OsmosisReader(new FileInputStream(input));
			reader.setSink(new Sink() {
				@Override
				public void release() { /* nothing to be done */ }			
				@Override
				public void complete() { complete = true; }
				@Override
				public void process(EntityContainer ec) {
					if (ec instanceof WayContainer) {
						
						Way w = ((WayContainer) ec).getEntity();
						Collection<Long> tileList = extraWayMap.get(w.getId());
						if (tileList != null)
							addExtraWayToMap(w, tileList);						
					}
				}
			});

			readerThread = new Thread(reader);
			readerThread.start();		
			while (readerThread.isAlive()) {
				try {
					readerThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			if (!complete)
				throw new IOException("Could not read file fully in second run");
		}
	}
	
	
	private boolean isInside(double x, double y, double[] polygon) {
		
		boolean in = false;
		int lines = polygon.length/2;
		
		for (int i = 0, j = lines-1; i < lines; j = i++) {
			
			if (((polygon[2*i+1]>y) != (polygon[2*j+1]>y)) &&
				(x < (polygon[2*j]-polygon[2*i]) * (y-polygon[2*i+1]) / (polygon[2*j+1]-polygon[2*i+1]) + polygon[2*i]))
				in = !in;
		}
		
		return in;
	}

	private boolean isInside(int tx, int ty, double[] polygon) {

		for (int u = 0; u < 2; u++) {
			for (int v = 0; v < 2; v++) {
				double x = tile2lon(tx+u);
				double y = tile2lat(ty+v);
				if (isInside(x, y, polygon))
					return true;
			}
		}
		return false;
	}
	
	private boolean isInside(int tx, int ty, List<double[]> inside, List<double[]> outside) {
		
		boolean in = false;
		for (double[] polygon : inside) {
			in |= isInside(tx, ty, polygon);
			if (in) break;
		}
		
		if (!in)
			return false;

		for (double[] polygon : outside) {
			if (isInside(tx, ty, polygon))
				return false;
		}
		
		return true;
	}
	
	public void clipPoly(String polygonFile) throws IOException {
		
		List<double[]> inside = new ArrayList<double[]>();
		List<double[]> outside = new ArrayList<double[]>();
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(polygonFile)));
		/*String name = */ br.readLine(); // unused..
		
		String poly = br.readLine();
		while (!"END".equals(poly)) {

			int pos = 0;
			int size = 128;
			double[] data = new double[2*size];
			
			String coords = br.readLine();
			while (!"END".equals(coords)) {
				
				coords = coords.trim();
				int idx = coords.indexOf(" ");
				double lon = Double.parseDouble(coords.substring(0, idx));
				double lat = Double.parseDouble(coords.substring(idx+1));
				
				// check if there's enough space to store
				if (pos >= size) {
					double[] tmp = new double[4*size];
					System.arraycopy(data, 0, tmp, 0, 2*size);
					size *= 2;
					data = tmp;
				}
				
				// store data
				data[2*pos] = lon;
				data[2*pos+1] = lat;
				pos++;
				
				coords = br.readLine();
			}

			if (pos != size) {
				double[] tmp = new double[2*pos];
				System.arraycopy(data, 0, tmp, 0, 2*pos);
				data = tmp;
			}

			if (poly.startsWith("!")) {
				outside.add(data);
			} else {
				inside.add(data);
			}
			
			// read next polygon, if there's any
			poly = br.readLine();
		}
		
		// now walk modifiedTiles and clear bits that are not inside polygon
		int idx = 0;
		while (true) {
			idx = modifiedTiles.nextSetBit(idx+1);
			if (idx == -1)
				break;
			
			int tx = idx >> 13;
			int ty = idx & 8191;
			
			boolean in = isInside(tx, ty, inside, outside);
			
			if (!in)
				modifiedTiles.clear(idx);
		}
	}
	
	public void store(String basename, boolean metadata, boolean verbose) throws IOException {

		int idx = 0;
		
		// We might call this code several times if we have more tiles
		// to store than open files allowed
		while (true) {

			complete = false;
			outFiles = new HashMap<Integer, OsmosisSerializer>();
		
			// Setup out-files...
			int count = 0; 
			while (true) {
				idx = modifiedTiles.nextSetBit(idx+1);
				if (idx == -1) {
					break;
				}
				
				if (outFiles.get(idx) == null) {

					int tileX = idx >> 13;
					int tileY = idx & 8191;
					String file;
					if (basename.contains("%x") && basename.contains("%y")) {
						file = basename.replace("%x", Integer.toString(tileX)).replace("%y", Integer.toString(tileY));
						if (!file.endsWith(".pbf"))
							file = file + ".pbf";
					} else {
						file = basename + tileX + "_" + tileY + ".pbf";
					}

					OsmosisSerializer serializer =
						new OsmosisSerializer(new BlockOutputStream(new FileOutputStream(file)));

					serializer.setUseDense(true);
					serializer.configOmit(!metadata);
				
					// write out the bound for that tile
					Bound bound = getBound(tileX, tileY);
					BoundContainer bc = new BoundContainer(bound);
					serializer.process(bc);
				
					outFiles.put(idx, serializer);
				}
				
				if ((maxFiles != -1) && (++count >= maxFiles))
					break;
			}
	
			// Now start writing output...
		
			RunnableSource reader =	new OsmosisReader(new FileInputStream(input));
			reader.setSink(new Sink() {
				@Override
				public void release() { 
					// nothing to be done 
				}			
				@Override
				public void complete() { complete = true; }
				@Override
				public void process(EntityContainer ec) {
					long id = ec.getEntity().getId();
					
					List<Integer> tiles;
					
					if (ec instanceof NodeContainer)
						tiles = nmap.getAllTiles(id);
					else if (ec instanceof WayContainer)
						tiles = wmap.getAllTiles(id);
					else if (ec instanceof RelationContainer) {
						tiles = rmap.getAllTiles(id);
					} else if (ec instanceof BoundContainer) {
						// nothing todo, we ignore bound tags
						return;
					} else {
						System.err.println("Unknown Element while reading");
						System.err.println(ec.toString());
						System.err.println(ec.getEntity().toString());
						return;
					}
				
					if (tiles == null) {
						// No tile where we could store the given entity into
						// This probably is a degenerated relation ;)
						return;
					}
				
					for (int i : tiles) {
						if (modifiedTiles.get(i)) {
							OsmosisSerializer ser = outFiles.get(i);
							if (ser != null)
								ser.process(ec);
						}
					}
				}
			});
		
			Thread readerThread = new Thread(reader);
			readerThread.start();
			while (readerThread.isAlive()) {
				try {
					readerThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		
			if (!complete)
				throw new IOException("Could not fully read file in storing run");
		
			// Finish and close files...
			for (Entry<Integer, OsmosisSerializer> entry : outFiles.entrySet()) {
				OsmosisSerializer ser = entry.getValue();
				
				ser.complete();
				ser.flush();
				ser.close();
			}
			
			if (idx == -1)
				break;
			
			if (verbose)
				System.out.println("Wrote " + maxFiles + " tiles, continuing with next block of tiles");
		}
	}
	
	
	
	/*
	 * Command line handling and main function
	 */
	
	
	private static Date run(String inputFile, 
			  		        String outputBase,
			  		        String polygonFile,
			  		        int[] mapSizes,
			  		        int maxFiles,
			  		        double border,
			 			    Date appointmentDate,
			 			    boolean metadata,
						    boolean verbose,
						    boolean timing,
						    boolean completeRelations) throws Exception {

		long startup = System.currentTimeMillis();

		MapSplit split = new MapSplit(appointmentDate, mapSizes, maxFiles, border, new File(inputFile), completeRelations);
		
		long time = System.currentTimeMillis();
		split.setup();
		time = System.currentTimeMillis() - time;
		
		double nratio = split.nmap.getMissHitRatio();
		double wratio = split.wmap.getMissHitRatio();
		double rratio = split.rmap.getMissHitRatio();
			
		if (polygonFile != null) {
			if (verbose)
				System.out.println("Clip tiles with polygon given by \"" + polygonFile + "\"");
			split.clipPoly(polygonFile);
		}
		
		int modified = split.modifiedTiles.cardinality();

		if (timing)
			System.out.println("Initial reading and datastructure setup took " + time + "ms");
		if (verbose)
			System.out.println("We have " + modified + " modified tiles to store.");
		
		time = System.currentTimeMillis();
		split.store(outputBase, metadata, verbose);
		time = System.currentTimeMillis() - time;
		if (timing) {
			System.out.println("Saving " + modified + " tiles took " + time + "ms");
			long overall = System.currentTimeMillis() - startup;
			System.out.print("\nOverall runtime: " + overall + "ms");
			System.out.println(" == " + (overall / 1000 / 60) + "min");
		}
		
		if (verbose) {
			System.out.println("\nHashmap's load:");
			System.out.println("Nodes    : " + split.nmap.getLoad());
			System.out.println("Ways     : " + split.wmap.getLoad());
			System.out.println("Relations: " + split.rmap.getLoad());
			System.out.println("\nHashmap's MissHitRatio:");
			System.out.printf("Nodes    : %10.6f\n", nratio);
			System.out.printf("Ways     : %10.6f\n", wratio);
			System.out.printf("Relations: %10.6f\n", rratio);
		}
		
		return split.latestDate;
	}
	
	
	private static void help() {
		System.out.println("Usage: mapsplit [options] <infile> <output base>");
		System.out.println("Mapsplit loads infile and stores any tile or tiles that got changed since a specified date in a tile file.\n");
		System.out.println("infile: A tile file in pbf format");
		System.out.println("output base: this is the base name of all tiles that will be written. The filename may contain '%x' and '%y' which will be replaced with the tilenumbers at zoom 13\n");

		System.out.println("Options:");
		System.out.println("  -h, --help         this help");
		System.out.println("  -v, --verbose      additional informational output");
		System.out.println("  -t, --timing       output timing information");
		System.out.println("  -m, --metadata     store metadata in tile-files (e.g. needed for JOSM)");
		System.out.println("  -c, --completeMPs  store complete data for multi polygons");
		System.out.println("  -f, --fd-max=val   maximum number of open files at a time");
		System.out.println("  -b, --border=val   enlarge tiles by val ([0-1[) of the tile's size to get a border around the tile.");
		System.out.println("  -p, --polygon=file only save tiles that intersect or lie within the given polygon file");
		System.out.println("  -d, --date=file    file containing the date since when tiles are being considered to have changed");
		System.out.println("                     after the split the latest change in infile is going to be stored in file");
		System.out.println("  -s, --size=n,w,r   the size for the node-, way- and relation maps to use (should be at least twice ");
		System.out.println("                     the number of IDs). If not supplied, defaults will be taken.");
	}
	

	
	public static void main(String[] args) throws Exception {
		
		int idx, mandatory = 0;
		Date appointmentDate;
		String inputFile = null;
		String outputBase = null;
		String polygonFile = null;
		boolean verbose = false;
		boolean timing = false;
		boolean metadata = false;
		boolean completeRelations = false;
		String dateFile = null;
		int[] mapSizes = new int[]{NODE_MAP_SIZE, WAY_MAP_SIZE, RELATION_MAP_SIZE};
		int maxFiles = -1;
		double border = 0.0;
		
		// Simple argument parser..
		for (int i = 0; i < args.length; i++) {
			
			if (args[i].startsWith("-")) {
				if (args[i].startsWith("--")) args[i] = args[i].substring(1);
				switch (args[i].charAt(1)) {
				case 'h':
					help();
					System.exit(0);
				case 'v':
					verbose = true;
					break;
				case 't':
					timing = true;
					break;
				case 'm':
					metadata = true;
					break;
				case 'c':
					completeRelations = true;
					break;
				case 'f':
					idx = args[i].indexOf('=');
					if (idx == -1) {
						System.out.println("Supply a number for max fds");
						help();
						System.exit(1);
					}
					String tmp = args[i].substring(idx+1);
					maxFiles = Integer.valueOf(tmp);
					break;
				case 'd':
					idx = args[i].indexOf('=');
					if (idx == -1) {
						System.err.println("Supply a date file");
						help();
						System.exit(1);
					}
					dateFile = args[i].substring(idx+1);
					break;
				case 'p':
					idx = args[i].indexOf('=');
					if (idx != -1) {
						polygonFile = args[i].substring(idx+1);
					}
					break;
				case 's':
					idx = args[i].indexOf('=');
					if (idx == -1) {
						System.err.println("Missing values for map sizes");
						help();
						System.exit(2);
					}
					tmp = args[i].substring(idx+1);
					String[] vals = tmp.split(",");
					for (int j = 0; j < 3; j++)
						mapSizes[j] = Integer.valueOf(vals[j]);
					break;
				case 'b':
					idx = args[i].indexOf('=');
					try {
						border = Double.valueOf(args[i].substring(idx+1));
						if (border < 0) border = 0;
						if (border > 1) border = 1;
					} catch (NumberFormatException e) {
						System.err.println("Could not parse border parameter, falling back to defaults");
					}
					break;
				}
			} else if (mandatory == 0) {
				inputFile = args[i];
				mandatory++;
			} else if (mandatory == 1) {
				outputBase = args[i];
				mandatory++;
			} else {
				System.err.println("Ignoring extra argument \"" + args[i] + "\"");
			}
		}
		
		if (mandatory < 2) {
			System.err.println("Please supply an input file and output basename");
			help();
			System.exit(3);
		}

		// Date-setup as fall-back option
		DateFormat df = DateFormat.getDateTimeInstance();
		appointmentDate = new Date(0);
		
		if (dateFile == null && verbose)
				System.out.println("No datefile given. Writing all available tiles.");		
		else if (dateFile != null) {
			
			File file = new File(dateFile);
			
			if (file.exists()) {
				DataInputStream dis = new DataInputStream(new FileInputStream(file));
				String line = dis.readUTF();
			
				if (line != null) {
					try {
						appointmentDate = df.parse(line);
					} catch (ParseException pe) {
						if (verbose)
							System.out.println("Could not parse datefile.");
					}
				}
				dis.close();
			} else if (verbose) {
				System.out.println("Datefile does not exist, writing all tiles");
			}
		}
		
		if (verbose) {
			System.out.println("Reading: " + inputFile);
			System.out.println("Writing: " + outputBase);
		}
		
		// Actually run the splitter... 
		Date latest = run(inputFile, outputBase, polygonFile, mapSizes, maxFiles, border, appointmentDate, 
                          metadata, verbose, timing, completeRelations);
		
		if (verbose)
			System.out.println("Last changes to the map had been done on " + df.format(latest));
		if (dateFile != null) {
			DataOutputStream dos = new DataOutputStream(new FileOutputStream(dateFile));
			dos.writeUTF(df.format(latest));
		}
	}	
}
