package org.apache.cassandra.stress.settings;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsNode
{

//    availableOptions.addOption("d",  "nodes",                true,   "Host nodes (comma separated), default:locahost");
//    availableOptions.addOption("D",  "nodesfile",            true,   "File containing host nodes (one per line)");

    public final List<String> nodes;

    public static final class Options extends GroupedOptions
    {
        final OptionSimple file = new OptionSimple("file=", ".*", null, "Node file (one per line)", false);
        final OptionSimple list = new OptionSimple("", "[^=,]+(,[^=,]+)*", "localhost", "comma delimited list of hosts", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(file, list);
        }
    }

    public SettingsNode(Options options)
    {
        if (options.file.present())
        {
            try
            {
                String node;
                List<String> tmpNodes = new ArrayList<String>();
                BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(options.file.value())));
                try
                {
                    while ((node = in.readLine()) != null)
                    {
                        if (node.length() > 0)
                            tmpNodes.add(node);
                    }
                    nodes = Arrays.asList(tmpNodes.toArray(new String[tmpNodes.size()]));
                }
                finally
                {
                    in.close();
                }
            }
            catch(IOException ioe)
            {
                throw new RuntimeException(ioe);
            }

        }
        else
            nodes = Arrays.asList(options.list.value().split(","));
    }

    public String randomNode()
    {
        int index = (int) (Math.random() * nodes.size());
        if (index >= nodes.size())
            index = nodes.size() - 1;
        return nodes.get(index);
    }

    public static SettingsNode get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.get("-node");
        if (params == null)
            return new SettingsNode(new Options());

        GroupedOptions options = GroupedOptions.select(params, new Options());
        if (options == null)
        {
            GroupedOptions.printOptions(System.out, new Options());
            throw new IllegalArgumentException("Invalid -node options provided, see output for valid options");
        }
        return new SettingsNode((Options) options);
    }


}
