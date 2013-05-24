package org.apache.cassandra.hadoop;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TTransport;

/**
 * Wraps an instance of client and allows to close the underlying transport
 * when the client is not needed anymore.
 */
public class ClientHolder
{
    public final Cassandra.Client thriftClient;
    public final TTransport transport;
    public final String host;

    public ClientHolder(Cassandra.Client thriftClient, TTransport transport, String host)
    {
        this.thriftClient = thriftClient;
        this.transport = transport;
        this.host = host;
    }

    public void close()
    {
        if (transport.isOpen())
            transport.close();
    }
}
