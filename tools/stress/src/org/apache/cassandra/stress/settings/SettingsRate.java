package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsRate implements Serializable
{

    public final boolean auto;
    public final int threadCount;
    public final int opRateTargetPerSecond;

    public SettingsRate(ThreadOptions options)
    {
        auto = false;
        threadCount = Integer.parseInt(options.threads.value());
        String rateOpt = options.rate.value();
        opRateTargetPerSecond = Integer.parseInt(rateOpt.substring(0, rateOpt.length() - 2));

    }

    public SettingsRate(AutoOptions auto)
    {
        this();
    }

    public SettingsRate()
    {
        this.auto = true;
        this.threadCount = -1;
        this.opRateTargetPerSecond = 0;
    }

    private static final class AutoOptions extends GroupedOptions
    {
        final OptionSimple auto = new OptionSimple("auto", "", null, "test with increasing number of threadCount until performance plateaus", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(auto);
        }
    }

    private static final class ThreadOptions extends GroupedOptions
    {
        final OptionSimple threads = new OptionSimple("threadCount=", "[0-9]+", null, "run this many clients concurrently", true);
        final OptionSimple rate = new OptionSimple("limit=", "[0-9]+/s", "0/s", "limit operations per second across all clients", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(threads, rate);
        }
    }

    public static SettingsRate get(Map<String, String[]> clArgs, SettingsCommand command)
    {
        String[] params = clArgs.remove("-rate");
        if (params == null)
        {
            if (command.type == Command.WRITE)
            {
                ThreadOptions options = new ThreadOptions();
                options.accept("threadCount=50");
                return new SettingsRate(options);
            }
            return new SettingsRate();
        }
        GroupedOptions options = GroupedOptions.select(params, new AutoOptions(), new ThreadOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -rate options provided, see output for valid options");
            System.exit(1);
        }
        if (options instanceof AutoOptions)
            return new SettingsRate((AutoOptions) options);
        else if (options instanceof ThreadOptions)
            return new SettingsRate((ThreadOptions) options);
        else
            throw new IllegalStateException();
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-rate", new AutoOptions(), new ThreadOptions());
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

