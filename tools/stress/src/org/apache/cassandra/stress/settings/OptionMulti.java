package org.apache.cassandra.stress.settings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class OptionMulti extends GroupedOptions implements Option
{

    private static final Pattern ARGS = Pattern.compile("([^,]+)", Pattern.CASE_INSENSITIVE);

    private final String name;
    private final Pattern pattern;
    private final String description;
    public OptionMulti(String name, String description)
    {
        this.name = name;
        pattern = Pattern.compile(name + "\\((.*)\\)");
        this.description = description;
    }

    @Override
    public boolean accept(String param)
    {
        Matcher m = pattern.matcher(param);
        if (!m.matches())
            return false;
        m = ARGS.matcher(m.group(1));
        int last = -1;
        while (m.find())
        {
            if (m.start() != last)
                throw new IllegalArgumentException("Invalid " + name + " specification: " + param);
            last = m.end() + 1;
            if (!super.accept(m.group()))
                throw new IllegalArgumentException("Invalid " + name + " specification: " + m.group());
        }
        return true;
    }

    @Override
    public String description()
    {
        return description;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("(");
        for (Option option : options())
        {
            sb.append(option);
            sb.append(",");
        }
        sb.append(")");
        return sb.toString();
    }

}
