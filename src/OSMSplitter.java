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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openstreetmap.osmosis.core.container.v0_6.BoundContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableSource;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.file.BlockOutputStream;
import crosby.binary.osmosis.OsmosisReader;
import crosby.binary.osmosis.OsmosisSerializer;


public class OSMSplitter {

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
	
	// the input file we're going to split
	private File input;
	
	// internal store to check if reading the file worked
	private boolean complete = false;

	// the hashmap for all nodes in the osm map
	private OsmMap nmap;

	// the hashmap for all ways in the osm map
	private OsmMap wmap;

	// the hashmap for all relations in the osm map
	private OsmMap rmap;

	// a bitset telling the algorithm which tiles need to be rerendered
	private BitSet modifiedTiles = new BitSet();
	
	// the serializer (OSM writers) for any modified tile
	private Map<Integer, OsmosisSerializer> outFiles;
	

	
	
	public OSMSplitter(Date appointmentDate, File file) {
		this.input = file;
		this.appointmentDate = appointmentDate;
		nmap = new HeapMap(NODE_MAP_SIZE);
		wmap = new HeapMap(WAY_MAP_SIZE);
		rmap = new HeapMap(RELATION_MAP_SIZE);
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
	
	private void addNodeToMap(Node n, double lat, double lon) {
		int tileX = lon2tileX(lon);
		int tileY = lat2tileY(lat);

		// TODO: check and add border
		// TODO: mark tile border to be rerendered
		
		// mark current tile to be rerendered
		if (n.getTimestamp().after(appointmentDate))
			modifiedTiles.set(tileX << 13 | tileY);
		
		// mark the latest changes made to this map
		if (n.getTimestamp().after(latestDate))
			latestDate = n.getTimestamp();
		
		nmap.put(n.getId(), tileX, tileY, OsmMap.NEIGHBOURS_NONE);
	}
	
	private void addWayToMap(Way way) {
		
		boolean modified = way.getTimestamp().after(appointmentDate);
		List<Long> tileList = new ArrayList<Long>();
		
		// mark the latest changes made to this map
		if (way.getTimestamp().after(latestDate))
			latestDate = way.getTimestamp();
		
		for (WayNode wayNode : way.getWayNodes()) {
			// get tileNrs for given node
			long tile = nmap.get(wayNode.getNodeId());
			
			// mark tiles as modified
			if (modified) {
				// TODO: missing border treatment...
				int tx = nmap.tileX(tile);
				int ty = nmap.tileY(tile);
				modifiedTiles.set(tx << 13 | ty);
			}
			
			tileList.add(tile);
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
	
	private void addRelationToMap(Relation r) {

		boolean modified = r.getTimestamp().after(appointmentDate);
		List<Long> tileList = new ArrayList<Long>();
		
		if (r.getTimestamp().after(latestDate))
			latestDate = r.getTimestamp();
		
		for (RelationMember m : r.getMembers()) {
		
			switch (m.getMemberType()) {
			case Node:
				long tile = nmap.get(m.getMemberId());
				
				// The referenced node is not in our data set
				if (tile == 0)
					return;

				int tx = nmap.tileX(tile);
				int ty = nmap.tileY(tile);
				// TODO: add neighbourhood handling

				// mark tiles as modified
				if (modified) {
					modifiedTiles.set(tx << 13 | ty);
				}
				
				tileList.add(tile);
				break;
				
			case Way:
				List<Integer> list = wmap.getAllTiles(m.getMemberId());
				
				// The referenced way is not in our data set
				if (list == null)
					return;
				
				if (modified) {
					for (Integer i : list)
						modifiedTiles.set(i);
				}

				// TODO: make this a bit more generic / nicer code :/
				for (int i : list)
					tileList.add(((long) i) << 38);
				break;
			}

			// Just in case, this can happen due to silly input data :'(
			if (tileList.isEmpty())
				return;
			
			long val = tileList.get(0);
			int tx = rmap.tileX(val);
			int ty = rmap.tileY(val);
			
			// put relation into map with a "random" base tile
			rmap.put(r.getId(), tx, ty, OsmMap.NEIGHBOURS_NONE);
			// update map so that the relation knows in which tiles it is needed
			rmap.update(r.getId(), tileList);
			
			// TODO I'm not sure yet if all nodes and all ways belonging to the
			//      given relation need to be in every tile that this relation
			//      is in. I.e. do we need a "complete-relation" setting as we
			//      do it for "complete-ways"??			
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
	}
	
	
	public void store(String basename) throws IOException {
		
		complete = false;
		outFiles = new HashMap<Integer, OsmosisSerializer>();
		
		// Setup out-files...
		int idx = 0; 
		while (true) {
			idx = modifiedTiles.nextSetBit(idx+1);
			if (idx == -1)
				break;
			
			if (outFiles.get(idx) == null) {

				String file = basename + (idx >> 13) + "_" + (idx & 8191) + ".pbf"; 
				//System.out.println(file);
			
				OsmosisSerializer serializer =
					new OsmosisSerializer(new BlockOutputStream(new FileOutputStream(file)));

				serializer.setUseDense(true);
				serializer.configOmit(true);
				
				outFiles.put(idx, serializer);
			}
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
						outFiles.get(i).process(ec);
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
			throw new IOException("Could not fully read file in second run");
		
		
		// Finish and close files...
		for (Entry<Integer, OsmosisSerializer> entry : outFiles.entrySet()) {
			OsmosisSerializer ser = entry.getValue();
	
			ser.complete();
			ser.flush();
			ser.close();
		}
	}
	
	
	
	/*
	 * Command line handling and main function
	 */
	
	
	private static Date run(String inputFile, 
			  		        String outputBase,
			 			    Date appointmentDate,
						    boolean verbose,
						    boolean timing) throws Exception {

		long startup = System.currentTimeMillis();

		OSMSplitter split = new OSMSplitter(appointmentDate, new File(inputFile));
		
		long time = System.currentTimeMillis();
		split.setup();
		time = System.currentTimeMillis() - time;
		
		int modified = split.modifiedTiles.cardinality();
		
		if (timing)
			System.out.println("Initial reading and datastructure setup took " + time + "ms");
		if (verbose)
			System.out.println("We have " + modified + " modified tiles to store.");
		
		time = System.currentTimeMillis();
		split.store(outputBase);
		time = System.currentTimeMillis() - time;
		if (timing) {
			System.out.println("Saving " + modified + " tiles took " + time + "ms");
			long overall = System.currentTimeMillis() - startup;
			System.out.print("\nOverall runtime: " + overall + "ms");
			System.out.println(" == " + (overall / 1000 / 60) + "min");
		}
		
		if (verbose) {
			System.out.println("\nHashmaps load:");
			System.out.println("Nodes    : " + split.nmap.getLoad());
			System.out.println("Ways     : " + split.wmap.getLoad());
			System.out.println("Relations: " + split.rmap.getLoad());
		}
		
		return split.latestDate;
	}
	
	
	private static void help() {
		System.out.println("Usage: OSMSplitter [options] <infile> <output base>");
		System.out.println("OSMSplitter loads infile and stores any tile that got changed during the last 2 days in a tile file.\n");
		System.out.println("infile: A tile file in pbf format");
		System.out.println("output base: this is the base name of all tiles that will be written. The filename will be preceeded with the tilenumber at Zoom 13\n");

		System.out.println("Options:");
		System.out.println("  -h, --help         this help");
		System.out.println("  -v, --verbose      additional informational output");
		System.out.println("  -t, --timing       output timing information");
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
		boolean verbose = false;
		boolean timing = false;
		String dateFile = null;
		int[] mapSizes = new int[]{NODE_MAP_SIZE, WAY_MAP_SIZE, RELATION_MAP_SIZE};
		
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
				case 'd':
					idx = args[i].indexOf('=');
					if (idx == -1) {
						System.err.println("Supply a date file");
						help();
						System.exit(1);
					}
					dateFile = args[i].substring(idx+1);
					break;
				case 's':
					idx = args[i].indexOf('=');
					if (idx == -1) {
						System.err.println("Missing values for map sizes");
						help();
						System.exit(2);
					}
					String tmp = args[i].substring(idx+1);
					String[] vals = tmp.split(",");
					for (int j = 0; j < 3; j++)
						mapSizes[j] = Integer.valueOf(vals[j]);
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
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(Calendar.DAY_OF_MONTH, -3);
		appointmentDate = c.getTime();

		
		if (dateFile == null && verbose)
				System.out.println("No datefile given. Checking for changes in the last three days.");		
		else {
			
			DataInputStream dis = new DataInputStream(new FileInputStream(dateFile));
			String line = dis.readLine();
			
			if (line != null)
				appointmentDate = df.parse(line);
			
			dis.close();
		}
		
		if (verbose) {
			System.out.println("Reading: " + inputFile);
			System.out.println("Writing: " + outputBase);
		}
		
		// Actually run the splitter... 
		Date latest = run(inputFile, outputBase, appointmentDate, verbose, timing);
		
		if (verbose)
			System.out.println("Last changes to the map had been done on " + df.format(latest));
		if (dateFile != null) {
			DataOutputStream dos = new DataOutputStream(new FileOutputStream(dateFile));
			dos.writeChars(df.format(latest));
		}
	}	
}
