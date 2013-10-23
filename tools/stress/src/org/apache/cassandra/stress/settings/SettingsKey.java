package org.apache.cassandra.stress.settings;

import org.apache.cassandra.stress.generatedata.DataGenHexFromDistribution;
import org.apache.cassandra.stress.generatedata.DataGenHexFromOpIndex;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.cassandra.stress.generatedata.KeyGen;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsKey implements Serializable
{

//    availableOptions.addOption("s",  "stdev",                true,   "Standard Deviation for gaussian read key generation, default:0.1");
//    availableOptions.addOption("r",  "random",               false,  "Use random key generator for read key generation (STDEV will have no effect), default:false");
//    availableOptions.addOption("F",  "num-different-keys",   true,   "Number of different keys (if < NUM-KEYS, the same key will re-used multiple times), default:NUM-KEYS");
    // key size

    private final int keySize;
    private final DistributionFactory distribution;
    private final long[] range;

    public SettingsKey(DistributionOptions options)
    {
        this.keySize = Integer.parseInt(options.size.value());
        this.distribution = options.dist.get();
        this.range = null;
    }

    public SettingsKey(PopulateOptions options)
    {
        this.keySize = Integer.parseInt(options.size.value());
        this.distribution = null;
        String[] bounds = options.populate.value().split("\\.\\.+");
        this.range = new long[] { Long.parseLong(bounds[0]), Long.parseLong(bounds[1]) };
    }

    private static final class DistributionOptions extends GroupedOptions
    {
        final OptionDistribution dist = new OptionDistribution("dist=", "GAUSSIAN(0..1000000)");
        final OptionSimple size = new OptionSimple("size=", "[0-9]+", "10", "Key size in bytes", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(dist, size);
        }
    }

    private static final class PopulateOptions extends GroupedOptions
    {
        final OptionSimple populate = new OptionSimple("populate=", "[0-9]+\\.\\.+[0-9]+", "0..1000000", "Populate all keys in sequence", true);
        final OptionSimple size = new OptionSimple("size=", "[0-9]+", "10", "Key size in bytes", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(populate, size);
        }
    }

    public KeyGen keyGenerator()
    {
        if (range != null)
            return new KeyGen(new DataGenHexFromOpIndex(range[0], range[1]), keySize);
        return new KeyGen(new DataGenHexFromDistribution(distribution.get()), keySize);
    }

    public static SettingsKey get(Map<String, String[]> clArgs, OpType opType)
    {
        String[] params = clArgs.get("-key");
        if (params == null)
        {
            // return defaults:
            if (opType == OpType.INSERT)
                return new SettingsKey(new PopulateOptions());
            return new SettingsKey(new DistributionOptions());
        }
        GroupedOptions options = GroupedOptions.select(params, new PopulateOptions(), new DistributionOptions());
        if (options == null)
        {
            GroupedOptions.printOptions(System.out, new PopulateOptions(), new DistributionOptions());
            throw new IllegalArgumentException("Invalid -key options provided, see output for valid options");
        }
        return options instanceof PopulateOptions ?
                new SettingsKey((PopulateOptions) options) :
                new SettingsKey((DistributionOptions) options);
    }

}

