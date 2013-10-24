package org.apache.cassandra.stress.settings;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptionMap extends Option
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
    public List<String> multiLineDisplay()
    {
        throw new NotImplementedException();
    }

    @Override
    public String longDisplay()
    {
        throw new NotImplementedException();
    }

    @Override
    public String shortDisplay()
    {
        throw new NotImplementedException();
    }
}
