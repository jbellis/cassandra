package org.apache.cassandra.stress.settings;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsLog
{

//    availableOptions.addOption("ns", "no-statistics",        false,  "Turn off the aggegate statistics that is normally output after completion.");
//    availableOptions.addOption("f",  "file",                 true,   "Write output to given file");
//    availableOptions.addOption("i",  "progress-interval",    true,   "Progress Report Interval (seconds), default:10");

    public final boolean noSummary;
    public final File file;
    public final int intervalMillis;

    public static final class Options extends GroupedOptions
    {
        final OptionSimple noSummmary = new OptionSimple("no-summary", "", null, "Disable printing of aggregate statistics at the end of a test", false);
        final OptionSimple outputFile = new OptionSimple("file=", ".*", null, "Log to a file", false);
        final OptionSimple interval = new OptionSimple("interval=", "[0-9]+", "1", "Log progress every <value> seconds", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(noSummmary, outputFile, interval);
        }
    }

    public SettingsLog(Options options)
    {
        this.noSummary = options.noSummmary.present();
        if (options.outputFile.present())
            this.file = new File(options.outputFile.value());
        else
            this.file = null;
        this.intervalMillis = 1000 * Integer.parseInt(options.interval.value());
        if (intervalMillis <= 0)
            throw new IllegalArgumentException("Log interval must be greater than zero");
    }

    public PrintStream getOutput() throws FileNotFoundException
    {
        return file == null ? new PrintStream(System.out) : new PrintStream(file);
    }

    public static SettingsLog get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-log");
        if (params == null)
            return new SettingsLog(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -log options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsLog((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-log", new Options());
    }

    public static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }
}
