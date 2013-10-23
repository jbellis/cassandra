package org.apache.cassandra.stress.settings;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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


    /**
     *


     -rate <auto>
        or threads=<N> [limit=<N>/s]
     -insert [options]
         []
         [n=<N> (default 1000000)]
         [retry=<N> (default 10)]
         [super]
         [ignore_errors]
     -key DISTRIBUTION
     -cols DISTRIBUTION or names="col1,col2,col3,.."
     -values DISTRIBUTION
     -mode thrift or [prepared] [native] cql[1-3]
         [CL=<consistency level>]
     -schema
     -nodes [file=<path>] [node1,node2,node3]
     -log file=<path> [no-summary]
     -ssl
     -port


     */

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
        TTransport transport = transportFactory.getTransport(socket);
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
        if (op.type == OpType.INSERT || op.type == OpType.COUNTER_ADD)
            schema.createKeySpaces(this);

    }

    public static StressSettings parse(String[] params)
    {

    }



}
