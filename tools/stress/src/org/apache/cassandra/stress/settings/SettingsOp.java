package org.apache.cassandra.stress.settings;

import org.apache.cassandra.thrift.ConsistencyLevel;

import java.util.Arrays;
import java.util.List;

public class SettingsOp
{

//    availableOptions.addOption("o",  "operation",            true,   "Operation to perform (INSERT, READ, READWRITE, RANGE_SLICE, INDEXED_RANGE_SLICE, MULTI_GET, COUNTER_ADD, COUNTER_GET), default:INSERT");
//    availableOptions.addOption("n",  "num-keys",             true,   "Number of keys, default:1000000");
//    availableOptions.addOption("g",  "keys-per-call",        true,   "Number of keys to get_range_slices or multiget per call, default:1000");

//    availableOptions.addOption("u",  "supercolumns",         true,   "Number of super columns per key, default:1");
//    availableOptions.addOption("y",  "family-type",          true,   "Column Family Type (Super, Standard), default:Standard");
//    availableOptions.addOption("K",  "keep-trying",          true,   "Retry on-going operation N times (in case of failure). positive integer, default:10");
//    availableOptions.addOption("k",  "keep-going",           false,  "Ignore errors inserting or reading (when set, --keep-trying has no effect), default:false");
//    availableOptions.addOption("e",  "consistency-level",    true,   "Consistency Level to use (ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY), default:ONE");

    public final OpType type;
    public final long count;
    public final int retries;
    public final boolean ignoreErrors;
    public final ConsistencyLevel consistencyLevel;
    public final double targetUncertainty;
    public final int minimumUncertaintyMeasurements;

    private static abstract class Options extends GroupedOptions
    {
        final OptionSimple retries = new OptionSimple("retries=", "[0-9]+", "10", "Number of retries to perform for each operation before failing", false);
        final OptionSimple ignoreErrors = new OptionSimple("ignore_errors=", "true|false", "false", "Do not print/log errors", false);
        final OptionSimple consistencyLevel = new OptionSimple("consistency_level=", "ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY", "ONE", "Consistency level to use", false);
    }

    private static class Count extends Options
    {

        final OptionSimple count = new OptionSimple("n=", "[0-9]+", null, "Number of operations to perform", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(count, retries, ignoreErrors, consistencyLevel);
        }
    }

    private static class Uncertainty extends Options
    {

        final OptionSimple uncertainty = new OptionSimple("uncertainty<", "0\\.[0-9]+", null, "Run until the standard error of the mean is below this fraction", true);
        final OptionSimple minMeasurements = new OptionSimple("n>", "[0-9]+", "30", "Run at least this many iterations", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(uncertainty, minMeasurements, retries, ignoreErrors, consistencyLevel);
        }
    }

    public SettingsOp(OpType type, GroupedOptions options)
    {
        this(type, (Options) options,
                options instanceof Count ? (Count) options : null,
                options instanceof Uncertainty ? (Uncertainty) options : null
        );
    }

    public SettingsOp(OpType type, Options options, Count count, Uncertainty uncertainty)
    {
        this.type = type;
        this.retries = Integer.parseInt(options.retries.value());
        this.ignoreErrors = Boolean.parseBoolean(options.ignoreErrors.value());
        this.consistencyLevel = ConsistencyLevel.valueOf(options.consistencyLevel.value());
        if (count != null)
        {
            this.count = Long.parseLong(count.count.value());
            this.targetUncertainty = -1;
            this.minimumUncertaintyMeasurements = -1;
        }
        else
        {
            this.count = -1;
            this.targetUncertainty = Double.parseDouble(uncertainty.uncertainty.value());
            this.minimumUncertaintyMeasurements = Integer.parseInt(uncertainty.minMeasurements.value());
        }
    }

}
