package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsMode implements Serializable
{

    public final ConnectionAPI api;
    public final CqlVersion cqlVersion;
    public final boolean useNativeProtocol;

    public SettingsMode(GroupedOptions options)
    {
        if (options instanceof Cql3Options)
        {
            cqlVersion = CqlVersion.CQL3;
            Cql3Options opts = (Cql3Options) options;
            useNativeProtocol = opts.useNative.setByUser();
            api = opts.usePrepared.setByUser() ? ConnectionAPI.CQL_PREPARED : ConnectionAPI.CQL;
        }
        else if (options instanceof Cql2Options)
        {
            cqlVersion = CqlVersion.CQL2;
            useNativeProtocol = false;
            Cql2Options opts = (Cql2Options) options;
            api = opts.usePrepared.setByUser() ? ConnectionAPI.CQL_PREPARED : ConnectionAPI.CQL;

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

    // Option Declarations

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

    // CLI Utility Methods

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
