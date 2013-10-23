package org.apache.cassandra.stress.settings;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class SettingsMultiOp extends SettingsOp
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

    public SettingsMultiOp(OpType type, Options options)
    {
        super(type, options.parent);
        this.keysAtOnce = Integer.parseInt(options.maxKeys.value());
    }

    public static SettingsOp build(OpType type, String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params, new Options(new Uncertainty()), new Options(new Count()));
        if (options == null)
        {
            GroupedOptions.printOptions(System.out, new Options(new Uncertainty()), new Options(new Count()));
            throw new IllegalArgumentException("Invalid " + type + " options provided, see output for valid options");
        }
        return new SettingsMultiOp(type, (Options) options);
    }

}
