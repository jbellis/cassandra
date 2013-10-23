package org.apache.cassandra.stress.settings;

import java.io.PrintStream;
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
            for (GroupedOptions grouping : groupings)
                grouping.accept(param);
        for (GroupedOptions grouping : groupings)
            if (grouping.happy())
                return grouping;
        return null;
    }

    public static void printOptions(PrintStream out, GroupedOptions... groupings)
    {
        for (GroupedOptions grouping : groupings)
        {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Option option : grouping.options())
            {
                if (first)
                    first = false;
                else
                    sb.append(" ");
                sb.append(option);
            }
            out.println(sb.toString());
        }
    }

}
