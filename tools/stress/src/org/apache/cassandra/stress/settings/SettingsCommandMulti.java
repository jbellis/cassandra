package org.apache.cassandra.stress.settings;

import java.util.ArrayList;
import java.util.List;

public class SettingsCommandMulti extends SettingsCommand
{

    static final class Options extends GroupedOptions
    {
        final GroupedOptions parent;
        Options(GroupedOptions parent)
        {
            this.parent = parent;
        }
        final OptionSimple maxKeys = new OptionSimple("at-once=", "[0-9]+", "1000", "Number of keys per operation", false);

        @Override
        public List<? extends Option> options()
        {
            final List<Option> options = new ArrayList<>();
            options.add(maxKeys);
            options.addAll(parent.options());
            return options;
        }
    }

    public final int keysAtOnce;

    public SettingsCommandMulti(Command type, Options options)
    {
        super(type, options.parent);
        this.keysAtOnce = Integer.parseInt(options.maxKeys.value());
    }

    public static SettingsCommand build(Command type, String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params, new Options(new Uncertainty()), new Options(new Count()));
        if (options == null)
        {
            printHelp(type);
            System.out.println("Invalid " + type + " options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommandMulti(type, (Options) options);
    }

    public static void printHelp(Command type)
    {
        GroupedOptions.printOptions(System.out, type.toString().toLowerCase(), new Options(new Uncertainty()), new Options(new Count()));
    }

    public static Runnable helpPrinter(final Command type)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp(type);
            }
        };
    }
}
