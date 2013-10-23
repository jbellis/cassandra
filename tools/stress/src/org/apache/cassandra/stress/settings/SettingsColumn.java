package org.apache.cassandra.stress.settings;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.stress.generatedata.DataGenFactory;
import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.cassandra.stress.generatedata.DistributionFixed;
import org.apache.cassandra.stress.generatedata.RowGen;
import org.apache.cassandra.stress.generatedata.RowGenDistributedSize;
import org.apache.commons.lang.*;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SettingsColumn implements Serializable
{

//    availableOptions.addOption("S",  "column-size",          true,   "Size of column values in bytes, default:34");
//    availableOptions.addOption("V",  "average-size-values",  false,  "Generate column values of average rather than specific size");
//    availableOptions.addOption("RC", "unique values per column",          true,   "Max number of unique rows, default:50");
//    availableOptions.addOption("c",  "columns",              true,   "Number of columns per key, default:5");
//    availableOptions.addOption("Q",  "query-names",          true,   "Comma-separated list of column names to retrieve from each row.");
//    availableOptions.addOption("U",  "comparator",           true,   "Column Comparator to use. Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");

    private static abstract class Options extends GroupedOptions
    {
        final OptionSimple superColumns = new OptionSimple("super=", "[0-9]+", "-1", "Number of super columns to use (no super columns used if not specified)", false);
        final OptionSimple comparator = new OptionSimple("comparator=", "TimeUUIDType|AsciiType|UTF8Type", "AsciiType", "Column Comparator to use", false);
        final OptionDistribution size = new OptionDistribution("size=", "FIXED(34)");
        final OptionDataGen generator = new OptionDataGen("data=", "REPEAT(10000)");
    }

    private static final class NameOptions extends Options
    {
        final OptionSimple name = new OptionSimple("names=", ".*", null, "Column names", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(name, superColumns, comparator, size);
        }
    }

    private static final class CountOptions extends Options
    {
        final OptionDistribution count = new OptionDistribution("n=", "FIXED(5)");

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(count, superColumns, comparator, size);
        }
    }

    public final int maxColumnsPerKey;
    public final List<ByteBuffer> names;
    public final String comparator;
    public final boolean useTimeUUIDComparator;
    public final int superColumns;
    public final boolean useSuperColumns;
    public final boolean variableColumnCount;


    private final DistributionFactory sizeDistribution;
    private final DistributionFactory countDistribution;
    private final DataGenFactory dataGenFactory;

    public SettingsColumn(GroupedOptions options)
    {
        this((Options) options,
                options instanceof NameOptions ? (NameOptions) options : null,
                options instanceof CountOptions ? (CountOptions) options : null
        );
    }

    public SettingsColumn(Options options, NameOptions name, CountOptions count)
    {
        sizeDistribution = options.size.get();
        maxColumnsPerKey = (int) sizeDistribution.get().maxValue();
        variableColumnCount = sizeDistribution.get().minValue() < maxColumnsPerKey;
        superColumns = Integer.parseInt(options.superColumns.value());
        dataGenFactory = options.generator.get();
        useSuperColumns = superColumns > 0;
        {
            comparator = options.comparator.value();
            AbstractType parsed = null;

            try
            {
                parsed = TypeParser.parse(comparator);
            }
            catch (Exception e)
            {
                System.err.println(e.getMessage());
                System.exit(1);
            }

            useTimeUUIDComparator = parsed instanceof TimeUUIDType;

            if (!(parsed instanceof TimeUUIDType || parsed instanceof AsciiType || parsed instanceof UTF8Type))
            {
                System.err.println("Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
                System.exit(1);
            }
        }
        if (name != null)
        {
            assert count == null;

            AbstractType comparator = null;
            try
            {
                comparator = TypeParser.parse(this.comparator);
            } catch (Exception e)
            {
                throw new IllegalStateException(e);
            }

            final String[] names = StringUtils.split(name.name.value(), ",");
            this.names = new ArrayList<>(names.length);

            for (String columnName : names)
                this.names.add(comparator.fromString(columnName));

            final int nameCount = this.names.size();
            countDistribution = new DistributionFactory()
            {
                @Override
                public Distribution get()
                {
                    return new DistributionFixed(nameCount);
                }
            };
        }
        else
        {
            this.countDistribution = count.count.get();
            this.names = null;
        }
    }

    public RowGen rowGen()
    {
        return new RowGenDistributedSize(dataGenFactory.get(), countDistribution.get(), sizeDistribution.get());
    }

}
