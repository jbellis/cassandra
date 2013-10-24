package org.apache.cassandra.stress.settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsMode
{

//    availableOptions.addOption("L",  "enable-cql",           false,  "Perform queries using CQL2 (Cassandra Query Language v 2.0.0)");
//    availableOptions.addOption("L3", "enable-cql3",          false,  "Perform queries using CQL3 (Cassandra Query Language v 3.0.0)");
//    availableOptions.addOption("b",  "enable-native-protocol",  false,  "Use the binary native protocol (only work along with -L3)");
//    availableOptions.addOption("P",  "use-prepared-statements", false, "Perform queries using prepared statements (only applicable to CQL).");

    public final ConnectionAPI api;
    public final CqlVersion cqlVersion;
    public final boolean useNativeProtocol;

    private static final class Cql3Options extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql3", "", null, "", true);
        final OptionSimple useNative = new OptionSimple("native", "", null, "", false);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(useNative, usePrepared, api);
        }
    }

    private static final class Cql2Options extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql2", "", null, "", true);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(usePrepared, api);
        }
    }

    private static final class ThriftOptions extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("thrift", "", null, "", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(api);
        }
    }

    public SettingsMode(GroupedOptions options)
    {
        if (options instanceof Cql3Options)
        {
            cqlVersion = CqlVersion.CQL3;
            Cql3Options opts = (Cql3Options) options;
            useNativeProtocol = opts.useNative.present();
            api = opts.usePrepared.present() ? ConnectionAPI.CQL_PREPARED : ConnectionAPI.CQL;
        }
        else if (options instanceof Cql2Options)
        {
            cqlVersion = CqlVersion.CQL2;
            useNativeProtocol = false;
            Cql2Options opts = (Cql2Options) options;
            api = opts.usePrepared.present() ? ConnectionAPI.CQL_PREPARED : ConnectionAPI.CQL;

        }
        else if (options instanceof ThriftOptions)
        {
            cqlVersion = CqlVersion.NOCQL;
            useNativeProtocol = false;
            api = ConnectionAPI.THRIFT;
        }
        else
            throw new IllegalStateException();
    }

    public static SettingsMode get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-mode");
        if (params == null)
            return new SettingsMode(new ThriftOptions());

        GroupedOptions options = GroupedOptions.select(params, new ThriftOptions(), new Cql2Options(), new Cql3Options());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -mode options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsMode(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-mode", new ThriftOptions(), new Cql2Options(), new Cql3Options());
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
