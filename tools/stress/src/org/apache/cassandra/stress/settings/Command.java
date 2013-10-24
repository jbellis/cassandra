package org.apache.cassandra.stress.settings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum Command
{

    READ(false, SettingsCommand.helpPrinter("read"), "Multiple concurrent reads - the cluster must first be populated by a write test", CommandCategory.BASIC),
    WRITE(true, SettingsCommand.helpPrinter("write"), "insert", "Multiple concurrent writes against the cluster", CommandCategory.BASIC),
    MIXED(true, SettingsCommandMixed.helpPrinter(), "Both reads and writes with configurable ratio and distribution - the cluster must first be populated by a write test", CommandCategory.MIXED, WRITE, READ),
    RANGE_SLICE(false, SettingsCommandMulti.helpPrinter("range_slice"), "Range slice queries - the cluster must first be populated by a write test", CommandCategory.MULTI),
    INDEXED_RANGE_SLICE(false, SettingsCommandMulti.helpPrinter("indexed_range_slice"), "Range slice queries through a secondary index. The cluster must first be populated by a write test, with indexing enabled.", CommandCategory.MULTI),
    READMULTI(false, SettingsCommandMulti.helpPrinter("readmulti"), "multi_read", "Multiple concurrent reads fetching multiple rows at once. The cluster must first be populated by a write test.", CommandCategory.MULTI),
    COUNTERWRITE(true, SettingsCommand.helpPrinter("counterwrite"), "counter_add", "Multiple concurrent updates of counters.", CommandCategory.BASIC),
    COUNTERREAD(false, SettingsCommand.helpPrinter("counterread"), "counter_get", "Multiple concurrent reads of counters. The cluster must first be populated by a counterwrite test.", CommandCategory.BASIC),
    HELP(false, SettingsMisc.helpHelpPrinter(), "-?", "Print help for a command or option", null)
    ;

    private static final Map<String, Command> LOOKUP;
    static
    {
        final Map<String, Command> lookup = new HashMap<>();
        for (Command cmd : values())
        {
            lookup.put(cmd.toString().toLowerCase(), cmd);
            if (cmd.extraName != null)
                lookup.put(cmd.extraName, cmd);
        }
        LOOKUP = lookup;
    }

    public static Command get(String command)
    {
        return LOOKUP.get(command.toLowerCase());
    }

    public final boolean updates;
    public final CommandCategory category;
    public final String extraName;
    public final String description;
    public final Runnable helpPrinter;
    private final Command[] warmups;

    Command(boolean updates, Runnable helpPrinter, String description, CommandCategory category)
    {
        this(updates, helpPrinter, null, description, category);
    }
    Command(boolean updates, Runnable helpPrinter, String extra, String description, CommandCategory category)
    {
        this.updates = updates;
        this.category = category;
        this.helpPrinter = helpPrinter;
        this.extraName = extra;
        this.description = description;
        this.warmups = new Command[]{this};
    }
    Command(boolean updates, Runnable helpPrinter, String description, CommandCategory category, Command... warmups)
    {
        this.updates = updates;
        this.category = category;
        this.helpPrinter = helpPrinter;
        this.extraName = null;
        this.description = description;
        this.warmups = warmups;
    }

    public List<Command> getWarmups()
    {
        return Arrays.asList(warmups);
    }

    public void printHelp()
    {
        helpPrinter.run();
    }

}
