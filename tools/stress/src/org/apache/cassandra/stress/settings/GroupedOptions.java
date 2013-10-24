package org.apache.cassandra.stress.settings;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public abstract class GroupedOptions
{

    public boolean accept(String param)
    {
        for (Option option : options())
            if (option.accept(param))
                return true;
        return false;
    }

    public boolean happy()
    {
        for (Option option : options())
            if (!option.happy())
                return false;
        return true;
    }

    public abstract List<? extends Option> options();

    public static GroupedOptions select(String[] params, GroupedOptions... groupings)
    {
        for (String param : params)
        {
            boolean accepted = false;
            for (GroupedOptions grouping : groupings)
                accepted |= grouping.accept(param);
            if (!accepted)
                throw new IllegalArgumentException("Invalid parameter " + param);
        }
        for (GroupedOptions grouping : groupings)
            if (grouping.happy())
                return grouping;
        return null;
    }

    public static void printOptions(PrintStream out, String command, GroupedOptions... groupings)
    {
        out.println();
        boolean firstRow = true;
        for (GroupedOptions grouping : groupings)
        {
            if (!firstRow)
            {
                out.println(" OR ");
            }
            firstRow = false;

            StringBuilder sb = new StringBuilder("Usage: " + command);
            for (Option option : grouping.options())
            {
                sb.append(" ");
                sb.append(option.shortDisplay());
            }
            out.println(sb.toString());
        }
        out.println();
        final HashSet<Option> printed = new HashSet<>();
        for (GroupedOptions grouping : groupings)
        {
            for (Option option : grouping.options())
            {
                if (printed.add(option))
                {
                    out.println("  " + option.longDisplay());
                    for (String row : option.multiLineDisplay())
                        out.println("      " + row);
                }
            }
        }
    }

    public static String formatLong(String longDisplay, String description)
    {
        return String.format("%-40s %s", longDisplay, description);
    }

    public static String formatHeaderLong(String longDisplay, String description)
    {
        return String.format("%-40s %s", longDisplay, description);
    }

    public static String formatMultiLine(String longDisplay, String description)
    {
        return String.format("%-36s %s", longDisplay, description);
    }

}
