package dev.osm.mapsplit;

import java.io.PrintStream;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * StreamHandler that flushes after each log message
 * @author simon
 *
 */
public class FlushStreamHandler extends StreamHandler {
    
    /**
     * Create a new handler
     * 
     * @param out where we want to print to
     * @param fmt how to format
     */
    public FlushStreamHandler(PrintStream out, SimpleFormatter fmt) {
        super(out, fmt);
    }

    @Override
    public synchronized void publish(final LogRecord record) {
        super.publish(record);
        flush();
    }
}
