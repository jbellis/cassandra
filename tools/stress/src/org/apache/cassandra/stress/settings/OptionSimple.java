package org.apache.cassandra.stress.settings;

import java.util.regex.Pattern;

public class OptionSimple implements Option
{

    final String prefix;
    final String defaultValue;
    final Pattern pattern;
    final String description;
    final boolean required;
    String value;

    public OptionSimple(String prefix, String pattern, String defaultValue, String description, boolean required)
    {
        this.prefix = prefix;
        this.pattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        this.defaultValue = defaultValue;
        this.description = description;
        this.required = required;
    }

    public boolean present()
    {
        return value != null;
    }

    public String value()
    {
        return value != null ? value : defaultValue;
    }

    public boolean accept(String param)
    {
        if (param.toLowerCase().startsWith(prefix))
        {
            if (value != null)
                throw new IllegalArgumentException("Suboption " + prefix + " has been specified more than once");
            String v = param.substring(prefix.length());
            if (!pattern.matcher(param).matches())
                throw new IllegalArgumentException("Invalid option " + param + "; must match pattern " + pattern);
            value = v;
            return true;
        }
        return false;
    }

    @Override
    public boolean happy()
    {
        return !required || value != null;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (!required)
            sb.append("[");
        sb.append(prefix);
        if (prefix.endsWith("="))
            sb.append("<value>");
        if (defaultValue != null)
        {
            sb.append(" (default=");
            sb.append(defaultValue);
            sb.append(")");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String description()
    {
        return description;
    }

}
