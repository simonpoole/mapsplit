package dev.osm.mapsplit;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * a set of command line parameters. Immutable after construction.
 */
final class CommandLineParams {

    private static final String OPT_OUTPUT                  = "o";
    private static final String LONG_OPT_OUTPUT             = "output";
    private static final String OPT_INPUT                   = "i";
    private static final String LONG_OPT_INPUT              = "input";
    private static final String OPT_POLYGON                 = "p";
    private static final String LONG_OPT_POLYGON            = "polygon";
    private static final String OPT_DATE                    = "d";
    private static final String LONG_OPT_DATE               = "date";
    private static final String OPT_TIMING                  = "t";
    private static final String LONG_OPT_TIMING             = "timing";
    private static final String OPT_METADATA                = "m";
    private static final String LONG_OPT_METADATA           = "metadata";
    private static final String OPT_COMPLETE_RELATIONS      = "c";
    private static final String LONG_OPT_COMPLETE_RELATIONS = "complete";
    private static final String OPT_COMPLETE_AREAS          = "C";
    private static final String LONG_OPT_COMPLETE_AREAS     = "complete-areas";
    private static final String OPT_MBTILES                 = "M";
    private static final String LONG_OPT_MBTILES            = "mbtiles";
    private static final String OPT_SIZE                    = "s";
    private static final String LONG_OPT_SIZE               = "size";
    private static final String OPT_MAXFILES                = "f";
    private static final String LONG_OPT_MAXFILES           = "maxfiles";
    private static final String OPT_BORDER                  = "b";
    private static final String LONG_OPT_BORDER             = "border";
    private static final String OPT_OPTIMIZE                = "O";
    private static final String LONG_OPT_OPTIMIZE           = "optimize";
    private static final String OPT_ZOOM                    = "z";
    private static final String LONG_OPT_ZOOM               = "zoom";
    private static final String OPT_HELP                    = "h";
    private static final String LONG_OPT_HELP               = "help";
    private static final String OPT_VERBOSE                 = "v";
    private static final String LONG_OPT_VERBOSE            = "verbose";

    private static final String COMMAND_NAME = "mapsplit";

    /** the input PBF file we're going to split */
    final @NotNull File inputFile;

    /** base filename */
    final @NotNull String outputBase;

    /** a file containing a coverage polygon */
    final @Nullable File polygonFile;

    /** only add changes from after the date stored in this file. Updated with latest change in the input. */
    final @Nullable File dateFile;

    /** verbose output */
    final boolean verbose;

    /** output timing information */
    final boolean timing;

    /** include metadata (timestamp etc) */
    final boolean metadata;

    /** include all relation member objects in every tile the relation is present in (very expensive) */
    final boolean completeRelations;

    /** include all multipolygon relation member objects in every tile the relation is present in (expensive) */
    final boolean completeAreas;

    /** generate a MBTiles format SQLite file instead of individual tiles */
    final boolean mbTiles;

    /** sizes of the maps for OSM objects */
    final @NotNull int[] mapSizes;

    /** maximum number of files/tiles to have open at the same time */
    final int maxFiles;

    /** the size of the border (in percent for a tile's height and width) for single tiles */
    final double border;

    /** zoom level to generate tiles for */
    final int zoom;

    /** if > 0 optimize tiles so that they contain at least nodeLimit Nodes */
    final int nodeLimit;

    /**
     * constructs a set of parameters from the program arguments.
     * 
     * @param args arguments as passed to a main method
     * @param logger the logger to use for warnings in case of invalid arguments
     * 
     * @throws IllegalArgumentException if the parameters do not allow a run
     */
    public CommandLineParams(@NotNull String[] args, @NotNull Logger logger) throws IllegalArgumentException {

        /* define the available arguments */

        Option helpOption = Option.builder(OPT_HELP).longOpt(LONG_OPT_HELP).desc("this help").build();
        Option verboseOption = Option.builder(OPT_VERBOSE).longOpt(LONG_OPT_VERBOSE).desc("verbose information during processing").build();
        Option timingOption = Option.builder(OPT_TIMING).longOpt(LONG_OPT_TIMING).desc("output timing information").build();
        Option metadataOption = Option.builder(OPT_METADATA).longOpt(LONG_OPT_METADATA)
                .desc("store metadata in the tiles (version, timestamp), if the input file is missing the metadata abort").build();
        Option completeRelationOption = Option.builder(OPT_COMPLETE_RELATIONS).longOpt(LONG_OPT_COMPLETE_RELATIONS)
                .desc("store complete data for all relations").build();
        Option completeAreaOption = Option.builder(OPT_COMPLETE_AREAS).longOpt(LONG_OPT_COMPLETE_AREAS).desc("store complete data for multi polygons").build();
        Option mbTilesOption = Option.builder(OPT_MBTILES).longOpt(LONG_OPT_MBTILES).desc("store in a MBTiles format sqlite database").build();
        Option maxFilesOption = Option.builder(OPT_MAXFILES).longOpt(LONG_OPT_MAXFILES).hasArg().desc("maximum number of open files at a time").build();
        Option borderOption = Option.builder(OPT_BORDER).longOpt(LONG_OPT_BORDER).hasArg()
                .desc("enlarge tiles by val ([0-1]) of the tile's size to get a border around the tile.").build();
        Option polygonOption = Option.builder(OPT_POLYGON).longOpt(LONG_OPT_POLYGON).hasArg()
                .desc("only save tiles that intersect or lie within the given polygon file.").build();
        Option dateOption = Option.builder(OPT_DATE).longOpt(LONG_OPT_DATE).hasArg().desc(
                "file containing the date since when tiles are being considered to have changed after the split the latest change in infile is going to be stored in file")
                .build();
        Option sizeOption = Option.builder(OPT_SIZE).longOpt(LONG_OPT_SIZE).hasArg().desc(
                "n,w,r the size for the node-, way- and relation maps to use (should be at least twice the number of IDs). If not supplied, defaults will be taken.")
                .build();
        Option inputOption = Option.builder(OPT_INPUT).longOpt(LONG_OPT_INPUT).hasArgs().desc("a file in OSM pbf format").required().build();
        Option outputOption = Option.builder(OPT_OUTPUT).longOpt(LONG_OPT_OUTPUT).hasArg().desc(
                "if creating a MBTiles files this is the name of the file, otherwise this is the base name of all tiles that will be written. The filename may contain '%x' and '%y' which will be replaced with the tilenumbers")
                .required().build();
        Option zoomOption = Option.builder(OPT_ZOOM).longOpt(LONG_OPT_ZOOM).hasArg()
                .desc("zoom level to create the tiles at must be between 0 and 16 (inclusive), default is 13").build();
        Option optimizeOption = Option.builder(OPT_OPTIMIZE).longOpt(LONG_OPT_OPTIMIZE).hasArg()
                .desc("optimize the tile stack, agrument is minimum number of Nodes a tile should contain, default is to not optimize").build();

        Options options = new Options();

        options.addOption(helpOption);
        options.addOption(verboseOption);
        options.addOption(timingOption);
        options.addOption(metadataOption);
        options.addOption(completeRelationOption);
        options.addOption(completeAreaOption);
        options.addOption(mbTilesOption);
        options.addOption(maxFilesOption);
        options.addOption(borderOption);
        options.addOption(polygonOption);
        options.addOption(dateOption);
        options.addOption(sizeOption);
        options.addOption(inputOption);
        options.addOption(outputOption);
        options.addOption(zoomOption);
        options.addOption(optimizeOption);

        /* parse the command line arguments */

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption(OPT_HELP)) {
                new HelpFormatter().printHelp(COMMAND_NAME, options);
            }

            inputFile = new File(line.getOptionValue(LONG_OPT_INPUT));
            outputBase = line.getOptionValue(LONG_OPT_OUTPUT);

            if (line.hasOption(OPT_POLYGON)) {
                polygonFile = new File(line.getOptionValue(LONG_OPT_POLYGON));
            } else {
                polygonFile = null;
            }

            if (line.hasOption(OPT_DATE)) {
                dateFile = new File(line.getOptionValue(LONG_OPT_DATE));
            } else {
                dateFile = null;
            }

            verbose = line.hasOption(OPT_VERBOSE);
            timing = line.hasOption(OPT_TIMING);
            metadata = line.hasOption(OPT_METADATA);
            completeRelations = line.hasOption(OPT_COMPLETE_RELATIONS);
            completeAreas = line.hasOption(OPT_COMPLETE_AREAS);
            mbTiles = line.hasOption(OPT_MBTILES);

            if (line.hasOption(OPT_SIZE)) {
                String tmp = line.getOptionValue(LONG_OPT_SIZE);
                String[] vals = tmp.split(",");
                mapSizes = new int[3];
                for (int j = 0; j < 3; j++) {
                    mapSizes[j] = Integer.valueOf(vals[j]);
                }
            } else {
                mapSizes = new int[] { Const.NODE_MAP_SIZE, Const.WAY_MAP_SIZE, Const.RELATION_MAP_SIZE };
            }

            if (line.hasOption(OPT_MAXFILES)) {
                String tmp = line.getOptionValue(LONG_OPT_MAXFILES);
                maxFiles = Integer.valueOf(tmp);
            } else {
                maxFiles = -1;
            }

            double border = 0.0;
            if (line.hasOption(OPT_BORDER)) {
                String tmp = line.getOptionValue(LONG_OPT_BORDER);
                try {
                    border = Double.valueOf(tmp);
                    if (border < 0) {
                        border = 0;
                    }
                    if (border > 1) {
                        border = 1;
                    }
                } catch (NumberFormatException e) {
                    logger.log(Level.WARNING, "Could not parse border parameter, falling back to defaults");
                }
            }
            this.border = border;

            if (line.hasOption(OPT_ZOOM)) {
                String tmp = line.getOptionValue(LONG_OPT_ZOOM);
                zoom = Integer.valueOf(tmp);
            } else {
                zoom = 13;
            }

            if (line.hasOption(OPT_OPTIMIZE)) {
                String tmp = line.getOptionValue(LONG_OPT_OPTIMIZE);
                nodeLimit = Integer.valueOf(tmp);
            } else {
                nodeLimit = 0;
            }

        } catch (ParseException | NumberFormatException exp) {
            logger.log(Level.WARNING, exp.getMessage());
            new HelpFormatter().printHelp(COMMAND_NAME, options);
            throw new IllegalArgumentException(exp);
        }
    }
}
