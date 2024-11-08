[![build status](https://github.com/simonpoole/mapsplit/actions/workflows/javalib.yml/badge.svg)](https://github.com/simonpoole/mapsplit/actions) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=mapsplit&metric=alert_status)](https://sonarcloud.io/dashboard?id=mapsplit) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=mapsplit&metric=coverage)](https://sonarcloud.io/dashboard?id=mapsplit) [![sonarcloud bugs](https://sonarcloud.io/api/project_badges/measure?project=mapsplit&metric=bugs)](https://sonarcloud.io/component_measures?id=mapsplit&metric=bugs) [![sonarcould maintainability](https://sonarcloud.io/api/project_badges/measure?project=mapsplit&metric=sqale_rating)](https://sonarcloud.io/component_measures?id=mapsplit&metric=Maintainability) [![sonarcloud security](https://sonarcloud.io/api/project_badges/measure?project=mapsplit&metric=security_rating)](https://sonarcloud.io/component_measures?id=mapsplit&metric=Security) [![sonarcloud reliability](https://sonarcloud.io/api/project_badges/measure?project=mapsplit&metric=reliability_rating)](https://sonarcloud.io/component_measures?id=mapsplit&metric=Reliability)

### General

MapSplit is a small application to split a larger OpenStreetMap data file into tiles. 

It arose by the need of a fast way to split a OSM map file for rendering 3D tiles with OSM2World (www.osm2world.org) and was originally 
created by Peter Barth in 2011.

While conceptually similar to "vector tiles" the tiles generated by MapSplit contain unprocessed, unmodified and complete, OSM data. 
The generated tiles are referentially complete with respect to way nodes if the input file was so too. If the _metadata_ option
is set, the tiles can be used as a read only OSM source for editors that support this format (for example [Vespucci](https://vespucci.io)).

### MBTiles output

Tiling larger regions at higher zoom levels will result in a large number (as in 100s of thousands) of files, this is
not only unwieldy, it is slow too. The preferred output format is [MBTiles](https://github.com/mapbox/mbtiles-spec) for such use
cases. 

To make the contents easily identifiable and usable by applications we set the following meta data fields:

* _format_ __application/vnd.openstreetmap.data+pbf__ (note this is not a registered mime type)
* _minzoom_ and _maxzoom_
* *latest_date* the timestamp of the youngest OSM element added, as the number of seconds since the UNIX epoch

You should __NOT__ confuse this format with Mapbox vector tiles that use PBF encoded tiles for rendering data generated from, among
other sources, OpenStreetMap.

### Limitations

The current implementation keeps the data structures required for assigning OSM objects to tiles in main memory. With 4GB of main memory 
you can parse maps with up to 100 million nodes.

The maximum zoom levels tiles can be produced at is 16, as x and y tile numbers are packed in a 32 bit integer during processing. 
Tiling large areas at zoom level 16 will create large numbers of tiles and should only be used with the optimization pass enabled. 

If you need to process more than ~2'147'483'647 elements (nodes, ways and relations separately), use 
the --max-ids options (set the values to the largest current element ids). While processing is then only limited by available memory, 
the maximum number of objects that can have an extended tile list (that is the object needs to be copied to more than tiles in the immediate vicinity) 
is limited to a bit more than 16 million. This restriction will likely be removed in 
an upcoming release.

Note: the incremental update feature likely doesn't really work and should be replaced. For smaller regions re-tiling from an updated 
source file is probably faster in any case.

### Legal

See COPYING for licence information.

OpenStreetMap and the magnifying glass logo are trademarks of the OpenStreetMap Foundation. The MapSplit application is not endorsed by or affiliated with the OpenStreetMap Foundation. 

### Usage

    -b,--border <arg>     enlarge tiles by val ([0-1]) of the tile's size to
                          get a border around the tile.
    -c,--complete         store complete data for relations (including multi-polygons)
    -C,--complete-areas   store complete data for multi-polygons
    -d,--date <arg>       file containing the date since when tiles are being
                          considered to have changed after the split the
                          latest change in infile is going to be stored in file
    -O,--optimize <arg>   try to merge tiles with less that <arg> nodes to larger
                          tiles (2'000 might be a good value for this) 
    -f,--maxfiles <arg>   maximum number of open files at a time
    -h,--help             this help
    -i,--input <arg>      a file in OSM pbf format
    -m,--metadata         store metadata in the tiles (version, timestamp), 
                          if the input file is missing the metadata abort
    -o,--output <arg>     if creating a MBTiles file this is the name of the
                          file, otherwise this is the base name of all tiles
                          that will be written. The filename may contain '%x'
                          and '%y' which will be replaced with the tile numbers, 
                          and '%z' that will be replaced with the tile zoom level.
    -p,--polygon <arg>    only save tiles that intersect or lie within the
                          given polygon file.
    -s,--size <arg>       n,w,r the initial size for the node-, way- and relation
                          maps to use (should be at least twice the number of
                          IDs). If not supplied, defaults will be used.
    -t,--timing           output timing information
    -v,--verbose          verbose information during processing
    -M,--mbtiles          store in a MBTiles format sqlite database
    -x,--max-ids <arg>    n,w,r the maximum id to allow in the node, way and
                          relation arrays. Using this option will cause
                          Mapsplit to use a different data structure that is
                          capable of scaling to the entire planet, but will 
                          use an amount of memory proportional to the values specified here.
    -z,--zoom <arg>       zoom level to create the tiles at must be between 0 (silly)
                          and 16 (inclusive), default is 13

### Examples

Note: from version 3.0 on the pre-built version from this repository requires a Java 11 runtime to work.

* Generate a 211MB large MBTile format MapSplit file with all the data, including metadata for the Iraq in a couple of minutes:

        java -Xmx6G -jar mapsplit-all-0.3.0.jar -tvMm -i iraq-latest.osm.pbf -o iraq.msf -f 2000 -z 16 -O 2000

* Generate a MBTile format MapSplit file with all the data and metadata for the city of Zurich that is suitable for use with [Vespucci](https://vespucci.io/):

        java -Xmx6G -jar mapsplit-all-0.3.0.jar -tvMm -i switzerland-padded.osm.pbf -o zurich.msf -f 2000 -z 15 -O 2000 -s 200000000,20000000,2000000 -p Zurich_AL8-AL8.poly

  The `.poly` file with the boundaries can for example be retrieved from the [OSM Admin Boundaries](https://wambachers-osm.website/boundaries/) service.
  
### Generated mapsplit files for selected regions

Daily updated mapsplit files can be found here [https://mapsplit.poole.ch/](https://mapsplit.poole.ch/).

### Building

We use gradle for building, no other system is currently supported.

* Build standalone jar file: `./gradlew fatJar` - the jar file can afterwards be found in `build/libs`.

### Testing

The current tests are rather superficial and need to be improved, the high coverage numbers are misleading.


 