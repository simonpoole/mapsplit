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

    private static final String COMMAND_NAME = "mapsplit";

    /** the input PBF file we're going to split */
    public final @NotNull File inputFile;

    /** base filename */
    public final @NotNull String outputBase;

    /** a file containing a coverage polygon */
    public final @Nullable File polygonFile;

    /** only add changes from after the date stored in this file. Updated with latest change in the input. */
    public final @Nullable File dateFile;

    /** verbose output */
    public final boolean verbose;

    /** output timing information */
    public final boolean timing;

    /** include metadata (timestamp etc) */
    public final boolean metadata;

    /** include all relation member objects in every tile the relation is present in (very expensive) */
    public final boolean completeRelations;

    /** include all multipolygon relation member objects in every tile the relation is present in (expensive) */
    public final boolean completeAreas;

    /** generate a MBTiles format SQLite file instead of individual tiles */
    public final boolean mbTiles;

    /** sizes of the maps for OSM objects */
    public final @NotNull int[] mapSizes;

    /** maximum number of files/tiles to have open at the same time */
    public final int maxFiles;

    /** the size of the border (in percent for a tile's height and width) for single tiles */
    public final double border;

    /** zoom level to generate tiles for */
    public final int zoom;

    /** if > 0 optimize tiles so that they contain at least nodeLimit Nodes */
    public final int nodeLimit;


    /**
     * constructs a set of parameters from the program arguments.
     * 
     * @param args  arguments as passed to a main method
     * @param logger  the logger to use for warnings in case of invalid arguments
     * 
     * @throws IllegalArgumentException  if the parameters do not allow a run
     */
    public CommandLineParams(String[] args, Logger logger) throws IllegalArgumentException {

        /* define the available arguments */

        Option helpOption = Option.builder("h").longOpt("help").desc("this help").build();
        Option verboseOption = Option.builder("v").longOpt("verbose").desc("verbose information during processing").build();
        Option timingOption = Option.builder("t").longOpt("timing").desc("output timing information").build();
        Option metadataOption = Option.builder("m").longOpt("metadata")
                .desc("store metadata in the tiles (version, timestamp), if the input file is missing the metadata abort").build();
        Option completeRelationOption = Option.builder("c").longOpt("complete").desc("store complete data for all relations").build();
        Option completeAreaOption = Option.builder("C").longOpt("complete-areas").desc("store complete data for multi polygons").build();
        Option mbTilesOption = Option.builder("M").longOpt("mbtiles").desc("store in a MBTiles format sqlite database").build();
        Option maxFilesOption = Option.builder("f").longOpt("maxfiles").hasArg().desc("maximum number of open files at a time").build();
        Option borderOption = Option.builder("b").longOpt("border").hasArg()
                .desc("enlarge tiles by val ([0-1]) of the tile's size to get a border around the tile.").build();
        Option polygonOption = Option.builder("p").longOpt("polygon").hasArg().desc("only save tiles that intersect or lie within the given polygon file.")
                .build();
        Option dateOption = Option.builder("d").longOpt("date").hasArg().desc(
                "file containing the date since when tiles are being considered to have changed after the split the latest change in infile is going to be stored in file")
                .build();
        Option sizeOption = Option.builder("s").longOpt("size").hasArg().desc(
                "n,w,r the size for the node-, way- and relation maps to use (should be at least twice the number of IDs). If not supplied, defaults will be taken.")
                .build();
        Option inputOption = Option.builder("i").longOpt("input").hasArgs().desc("a file in OSM pbf format").required().build();
        Option outputOption = Option.builder("o").longOpt("output").hasArg().desc(
                "if creating a MBTiles files this is the name of the file, otherwise this is the base name of all tiles that will be written. The filename may contain '%x' and '%y' which will be replaced with the tilenumbers")
                .required().build();
        Option zoomOption = Option.builder("z").longOpt("zoom").hasArg()
                .desc("zoom level to create the tiles at must be between 0 and 16 (inclusive), default is 13").build();

        Option optimizeOption = Option.builder("O").longOpt("optimize").hasArg()
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
            if (line.hasOption("h")) {
                new HelpFormatter().printHelp(COMMAND_NAME, options);
            }

            inputFile = new File(line.getOptionValue("input"));
            outputBase = line.getOptionValue("output");

            if (line.hasOption("p")) {
                polygonFile = new File(line.getOptionValue("polygon"));
            } else {
                polygonFile = null;
            }

            if (line.hasOption("d")) {
                dateFile = new File(line.getOptionValue("date"));
            } else {
                dateFile = null;
            }

            verbose = line.hasOption("v");
            timing = line.hasOption("t");
            metadata = line.hasOption("m");
            completeRelations = line.hasOption("c");
            completeAreas = line.hasOption("C");
            mbTiles = line.hasOption("M");

            if (line.hasOption("s")) {
                String tmp = line.getOptionValue("size");
                String[] vals = tmp.split(",");
                mapSizes = new int[3];
                for (int j = 0; j < 3; j++) {
                    mapSizes[j] = Integer.valueOf(vals[j]);
                }
            } else {
                mapSizes = new int[] { Const.NODE_MAP_SIZE, Const.WAY_MAP_SIZE, Const.RELATION_MAP_SIZE };
            }

            if (line.hasOption("f")) {
                String tmp = line.getOptionValue("maxfiles");
                maxFiles = Integer.valueOf(tmp);
            } else {
                maxFiles = -1;
            }

            double border = 0.0;
            if (line.hasOption("b")) {
                String tmp = line.getOptionValue("border");
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

            if (line.hasOption("z")) {
                String tmp = line.getOptionValue("zoom");
                zoom = Integer.valueOf(tmp);
            } else {
                zoom = 13;
            }

            if (line.hasOption("O")) {
                String tmp = line.getOptionValue("optimize");
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
