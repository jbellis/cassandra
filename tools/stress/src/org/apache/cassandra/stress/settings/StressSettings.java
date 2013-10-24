package org.apache.cassandra.stress.settings;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StressSettings
{

    public final SettingsCommand op;
    public final SettingsRate rate;
    public final SettingsKey keys;
    public final SettingsColumn columns;
    public final SettingsLog log;
    public final SettingsMode mode;
    public final SettingsNode node;
    public final SettingsSchema schema;
    public final SettingsTransport transport;
    private final int port;
    public final String sendToDaemon;

    public StressSettings(SettingsCommand op, SettingsRate rate, SettingsKey keys, SettingsColumn columns, SettingsLog log, SettingsMode mode, SettingsNode node, SettingsSchema schema, SettingsTransport transport, int port, String sendToDaemon)
    {
        this.op = op;
        this.rate = rate;
        this.keys = keys;
        this.columns = columns;
        this.log = log;
        this.mode = mode;
        this.node = node;
        this.schema = schema;
        this.transport = transport;
        this.port = port;
        this.sendToDaemon = sendToDaemon;
    }

    /**
     * Thrift client connection with Keyspace1 set.
     * @return cassandra client connection
     */
    public Cassandra.Client getClient()
    {
        return getClient(true);
    }

    /**
     * Thrift client connection
     * @param setKeyspace - should we set keyspace for client or not
     * @return cassandra client connection
     */
    public Cassandra.Client getClient(boolean setKeyspace)
    {
        // random node selection for fake load balancing
        String currentNode = node.randomNode();

        TSocket socket = new TSocket(currentNode, port);
        TTransport transport = this.transport.factory.getTransport(socket);
        Cassandra.Client client = new Cassandra.Client(new TBinaryProtocol(transport));

        try
        {
            if(!transport.isOpen())
                transport.open();

            if (mode.cqlVersion.isCql())
                client.set_cql_version(mode.cqlVersion.connectVersion);

            if (setKeyspace)
            {
                client.set_keyspace("Keyspace1");
            }
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e.getWhy());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }

        return client;
    }

    public SimpleClient getNativeClient()
    {
        try
        {
            String currentNode = node.randomNode();
            SimpleClient client = new SimpleClient(currentNode, 9042);
            client.connect(false);
            client.execute("USE \"Keyspace1\";", org.apache.cassandra.db.ConsistencyLevel.ONE);
            return client;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    public void maybeCreateKeyspaces()
    {
        if (op.type == Command.WRITE || op.type == Command.COUNTERWRITE)
            schema.createKeySpaces(this);

    }

    public static StressSettings parse(String[] args)
    {
        final Map<String, String[]> clArgs = parseMap(args);
        if (clArgs.containsKey("legacy"))
            return Legacy.build(Arrays.copyOfRange(args, 1, args.length));
        if (maybePrintHelp(clArgs))
            System.exit(1);
        return get(clArgs);
    }

    public static StressSettings get(Map<String, String[]> clArgs)
    {
        SettingsCommand command = SettingsCommand.get(clArgs);
        if (command == null)
            throw new IllegalArgumentException("No operation specified");
        int port = SettingsMisc.getPort(clArgs);
        String sendToDaemon = SettingsMisc.getSendToDaemon(clArgs);
        SettingsRate rate = SettingsRate.get(clArgs, command);
        SettingsKey keys = SettingsKey.get(clArgs, command);
        SettingsColumn columns = SettingsColumn.get(clArgs);
        SettingsLog log = SettingsLog.get(clArgs);
        SettingsMode mode = SettingsMode.get(clArgs);
        SettingsNode node = SettingsNode.get(clArgs);
        SettingsSchema schema = SettingsSchema.get(clArgs);
        SettingsTransport transport = SettingsTransport.get(clArgs);
        if (!clArgs.isEmpty())
        {
            printHelp();
            System.out.println("Error processing command line arguments. The following were ignored:");
            for (Map.Entry<String, String[]> e : clArgs.entrySet())
            {
                System.out.print(e.getKey());
                for (String v : e.getValue())
                {
                    System.out.print(" ");
                    System.out.print(v);
                }
                System.out.println();
            }
            System.exit(1);
        }
        return new StressSettings(command, rate, keys, columns, log, mode, node, schema, transport, port, sendToDaemon);
    }

    private static final Map<String, String[]> parseMap(String[] args)
    {
        // first is the main command/operation, so specified without a -
        if (args.length == 0)
            throw new IllegalArgumentException("Invalid command line arguments - none provided!");
        final LinkedHashMap<String, String[]> r = new LinkedHashMap<>();
        String key = null;
        List<String> params = new ArrayList<>();
        for (int i = 0 ; i < args.length ; i++)
        {
            if (i == 0 || args[i].startsWith("-"))
            {
                if (i > 0)
                    r.put(key, params.toArray(new String[0]));
                key = args[i].toLowerCase();
                params.clear();
            }
            else
                params.add(args[i]);
        }
        r.put(key, params.toArray(new String[0]));
        return r;
    }

    private static boolean maybePrintHelp(Map<String, String[]> clArgs)
    {
        if (!clArgs.containsKey("-?") && !clArgs.containsKey("help"))
            return false;
        String[] params = clArgs.remove("-?");
        if (params == null)
            params = clArgs.remove("help");
        if (params.length == 0)
        {
            if (!clArgs.isEmpty())
            {
                if (clArgs.size() == 1)
                {
                    String p = clArgs.keySet().iterator().next();
                    if (clArgs.get(p).length == 0)
                        params = new String[] {p};
                }
            }
            else
            {
                printHelp();
                return true;
            }
        }
        if (params.length == 1)
        {
            printHelp(params[0]);
            return true;
        }
        throw new IllegalArgumentException("Invalid command/option provided to help");
    }

    public static void printHelp()
    {
        System.out.println("Usage: ./bin/cassandra-stress <command> [options]");
        System.out.println();
        System.out.println("---Commands---");
        for (Command cmd : Command.values())
        {
            System.out.println(String.format("%-20s : %s", cmd.toString().toLowerCase(), cmd.description));
        }
        System.out.println();
        System.out.println("---Options---");
        for (CliOption cmd : CliOption.values())
        {
            System.out.println(String.format("-%-20s : %s", cmd.toString().toLowerCase(), cmd.description));
        }
    }

    public static void printHelp(String command)
    {
        Command cmd = Command.get(command);
        if (cmd != null)
        {
            cmd.printHelp();
            return;
        }
        CliOption opt = CliOption.get(command);
        if (opt != null)
        {
            opt.printHelp();
            return;
        }
        printHelp();
        throw new IllegalArgumentException("Invalid command or option provided to command help");
    }

}
