package org.apache.cassandra.stress.settings;

import java.util.List;

public class SettingsNode
{

//    availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
//    availableOptions.addOption("D",  "nodesfile",            true,   "File containing host nodes (one per line)");

    public final List<String> nodes;

    public SettingsNode(List<String> nodes)
    {
        this.nodes = nodes;
    }

    public String randomNode()
    {
        int index = (int) (Math.random() * nodes.size());
        if (index >= nodes.size())
            index = nodes.size() - 1;
        return nodes.get(index);
    }


}
