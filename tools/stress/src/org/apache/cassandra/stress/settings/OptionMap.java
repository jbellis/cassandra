package org.apache.cassandra.stress.settings;

import java.util.HashMap;
import java.util.Map;

public class OptionMap implements Option
{

    final Map<String, String> map = new HashMap<>();

    public boolean accept(String param)
    {
        String[] pair = param.split("=");
        if (pair.length != 2)
            return false;
        map.put(pair[0], pair[1]);
        return true;
    }

    @Override
    public boolean happy()
    {
        return true;
    }

    @Override
    public String description()
    {
        return "key1=value1,key2=value2...";
    }
}
