package org.apache.cassandra.stress.settings;

import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.commons.math3.distribution.*;
import org.apache.commons.math3.util.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SettingsCommandMixed extends SettingsCommand
{

    private final double readProbability;
    private final double writeProbability;
    private final DistributionFactory clustering;

    static final class Probabilities extends OptionMulti
    {
        final OptionSimple read = new OptionSimple("reads=", "[0-9.]+", "1", "Reads for every x writes", false);
        final OptionSimple write = new OptionSimple("writes=", "[0-9.]+", "1", "Writes for every x reads", false);

        public Probabilities()
        {
            super("ratio", "Specify a ratio of reads to writes; e.g. ratio(reads=2,writes=1) will perform 2 reads for every 1 write");
        }

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(read, write);
        }
    }

    static final class Options extends GroupedOptions
    {
        final GroupedOptions parent;
        Options(GroupedOptions parent)
        {
            this.parent = parent;
        }
        final OptionDistribution clustering = new OptionDistribution("clustering=", "NORMAL(1..10)");
        final Probabilities probabilities = new Probabilities();

        @Override
        public List<? extends Option> options()
        {
            final List<Option> options = new ArrayList<>();
            options.add(clustering);
            options.add(probabilities);
            options.addAll(parent.options());
            return options;
        }

    }

    public SettingsCommandMixed(Options options)
    {
        super(Command.MIXED, options.parent);
        readProbability = Double.parseDouble(options.probabilities.read.value());
        writeProbability = Double.parseDouble(options.probabilities.write.value());
        clustering = options.clustering.get();
    }

    public static final class ReadWriteSelector
    {

        final EnumeratedDistribution<Command> selector;
        final Distribution count;
        private Command cur;
        private long remaining;

        public ReadWriteSelector(double readProbability, double writeProbability, Distribution count)
        {
            selector = new EnumeratedDistribution<>(
                    Arrays.asList(
                            new Pair<>(Command.READ, readProbability),
                            new Pair<>(Command.WRITE, writeProbability)
                    ));
            this.count = count;
        }

        public Command next()
        {
            while (remaining == 0)
            {
                remaining = count.next();
                cur = selector.sample();
            }
            remaining--;
            return cur;
        }
    }

    public ReadWriteSelector selector()
    {
        return new ReadWriteSelector(readProbability, writeProbability, clustering.get());
    }

    public static SettingsCommandMixed build(String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params, new Options(new Uncertainty()), new Options(new Count()));
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid MIXED options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandMixed((Options) options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "mixed", new Options(new Uncertainty()), new Options(new Count()));
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
