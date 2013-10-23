package org.apache.cassandra.stress.settings;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsRate
{

    public final boolean auto;
    public final int threads;
    public final int opLimitPerSecond;

    public SettingsRate(ThreadOptions options)
    {
        auto = false;
        threads = Integer.parseInt(options.threads.value());
        String rateOpt = options.rate.value();
        opLimitPerSecond = Integer.parseInt(rateOpt.substring(0, rateOpt.length() - 2));

    }

    public SettingsRate(AutoOptions auto)
    {
        this();
    }

    public SettingsRate()
    {
        this.auto = true;
        this.threads = -1;
        this.opLimitPerSecond = -1;
    }

    private static final class AutoOptions extends GroupedOptions
    {
        final OptionSimple auto = new OptionSimple("auto", "", null, "test with increasing number of threads until performance plateaus", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(auto);
        }
    }

    private static final class ThreadOptions extends GroupedOptions
    {
        final OptionSimple threads = new OptionSimple("threads=", "[0-9]+", null, "run this many clients concurrently", true);
        final OptionSimple rate = new OptionSimple("limit=", "[0-9]+/s", null, "limit operations per second across all clients", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(threads, rate);
        }
    }

    public static SettingsRate get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.get("-rate");
        if (params == null)
            return new SettingsRate();
        GroupedOptions options = GroupedOptions.select(params, new AutoOptions(), new ThreadOptions());
        if (options == null)
        {
            GroupedOptions.printOptions(System.out, new AutoOptions(), new ThreadOptions());
            throw new IllegalArgumentException("Invalid -rate options provided, see output for valid options");
        }
        if (options instanceof AutoOptions)
            return new SettingsRate((AutoOptions) options);
        else if (options instanceof ThreadOptions)
            return new SettingsRate((ThreadOptions) options);
        else
            throw new IllegalStateException();
    }

}

