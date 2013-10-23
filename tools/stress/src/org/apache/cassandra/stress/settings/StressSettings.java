package org.apache.cassandra.stress.settings;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StressSettings
{

//    availableOptions.addOption("h",  "help",                 false,  "Show this help message and exit");
//    availableOptions.addOption("T",  "send-to",              true,   "Send this as a request to the stress daemon at specified address.");
//    availableOptions.addOption("p",  "port",                 true,   "Thrift port, default:9160");
//

    public final SettingsOp op;
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

    public StressSettings(SettingsOp op, SettingsRate rate, SettingsKey keys, SettingsColumn columns, SettingsLog log, SettingsMode mode, SettingsNode node, SettingsSchema schema, SettingsTransport transport, int port, String sendToDaemon)
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
        TTransport transport = this.transport.transportFactory.getTransport(socket);
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
        if (op.type == OpType.INSERT || op.type == OpType.COUNTERWRITE)
            schema.createKeySpaces(this);

    }

    public static StressSettings parse(String[] args)
    {
        try
        {
            final Map<String, String[]> map = parseMap(args);

            SettingsOp op = null;
            int port = 9160;
            String sendToDaemon = null;
            for (Map.Entry<String, String[]> e : map.entrySet())
            {
                switch (e.getKey().toLowerCase())
                {
                    case "read":
                    case "write":
                    case "counterwrite":
                    case "counterread":
                    case "range_slice":
                        op = SettingsOp.build(OpType.valueOf(e.getKey().toUpperCase()), e.getValue());
                    break;
                    case "mixed":
                        op = SettingsMixedOp.build(e.getValue());
                        break;
                    case "indexed_range_slice":
                    case "readmulti":
                        op = SettingsMultiOp.build(OpType.valueOf(e.getKey().toUpperCase()), e.getValue());
                        break;
                    case "help":
                        if (e.getValue().length > 0)
                            printHelp(e.getValue()[0]);
                        printHelp();
                        System.exit(1);
                        break;
                    case "port":
                        if (e.getValue().length != 1)
                            throw new IllegalArgumentException("Illegal port specifier: " + Arrays.toString(e.getValue()));
                        port = Integer.parseInt(e.getValue()[0]);
                        break;
                    case "sendToDaemon":
                        if (e.getValue().length != 1)
                            throw new IllegalArgumentException("Illegal sendToDaemon specifier: " + Arrays.toString(e.getValue()));
                        sendToDaemon = e.getValue()[0];
                        break;
                }
            }
            if (op == null)
                throw new IllegalArgumentException("No operation specified");
            SettingsRate rate = SettingsRate.get(map);
            SettingsKey keys = SettingsKey.get(map, op.type);
            SettingsColumn columns = SettingsColumn.get(map);
            SettingsLog log = SettingsLog.get(map);
            SettingsMode mode = SettingsMode.get(map);
            SettingsNode node = null;
            SettingsSchema schema = SettingsSchema.get(map);
            SettingsTransport transport = new SettingsTransport();
            return new StressSettings(op, rate, keys, columns, log, mode, node, schema, transport, port, sendToDaemon);
        } catch (IllegalArgumentException e)
        {
            printHelp();
            System.exit(1);
            throw new IllegalStateException();
        }
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
            if (i == 0 || args[0].startsWith("-"))
            {
                if (i > 0)
                    r.put(key, params.toArray(new String[0]));
                key = args[i];
                params.clear();
            }
            else
                params.add(args[i]);
        }
        r.put(key, params.toArray(new String[0]));
        return r;
    }

    private static void printHelp()
    {

    }

    private static void printHelp(String command)
    {

    }

}
