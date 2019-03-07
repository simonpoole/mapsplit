
### General

MapSplit is a small application to split a larger map file into single tiles at a single zoom level. 

It arose by the need of a fast way to split a OSM map file for rendering 3D tiles with OSM2World (www.osm2world.org). Only
tiles that have changes in them are stored afterwards.

### Limitations

The current implementation keeps everything that's needed in main memory. With 4GB of main memory you can parse maps with up to 100
million nodes, however, memory usage also depends on the number of tiles that got changed.

The maximum zoom levels tiles can be produced at is 16 as x and y tile numbers are packed in a 32 bit integer during processing. 
Tiling large areas at zoom level 16 will create large numbers of tiles and should only be used with the optimization pass enabled. 

The incremental update feature likely doesn't really work and should be replaced by something else.

### Legal

See COPYING.

### Usage

    -b,--border <arg>     enlarge tiles by val ([0-1]) of the tile's size to
                          get a border around the tile.
    -c,--complete         store complete data for multi-polygons
    -d,--date <arg>       file containing the date since when tiles are being
                          considered to have changed after the split the
                          latest change in infile is going to be stored in file
    -e,--optimize <arg>   try to merge tiles with less that <arg> nodes to larger
                          tiles (2'000 might be a good value for this) 
    -f,--maxfiles <arg>   maximum number of open files at a time
    -h,--help             this help
    -i,--input <arg>      a file in OSM pbf format
    -m,--metadata         store metadata in tile-files (version, timestamp)
    -o,--output <arg>     if creating a MBTiles files this is the name of the
                          file, otherwise this is the base name of all tiles
                          that will be written. The filename may contain '%x'
                          and '%y' which will be replaced with the tilenumbers
    -p,--polygon <arg>    only save tiles that intersect or lie within the
                          given polygon file.
    -s,--size <arg>       n,w,r the size for the node-, way- and relation
                          maps to use (should be at least twice the number of
                          IDs). If not supplied, defaults will be taken.
    -t,--timing           output timing information
    -v,--verbose          verbose information during processing
    -x,--mbtiles          store in a MBTiles format sqlite database
    -z,--zoom <arg>       zoom level to create the tiles at must be between 0 (silly)
                          and 16 (inclusive), default is 13
