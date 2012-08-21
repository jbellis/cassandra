package org.apache.cassandra.tracing;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

public class TracingAppender extends AppenderSkeleton
{
    protected void append(final LoggingEvent event)
    {
        final TraceState state = Tracing.instance().get();
        if (state == null)
            return;

        final int elapsed = state.elapsed();
        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow() throws TimedOutException, UnavailableException
            {
                ColumnFamily cf = ColumnFamily.create(CFMetaData.TraceEventsCf);
                addColumn(cf, "event_id", ByteBufferUtil.bytes(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress())));
                addColumn(cf, "source", FBUtilities.getBroadcastAddress());
                addColumn(cf, "thread", event.getThreadName());
                addColumn(cf, "happened_at", event.getTimeStamp());
                addColumn(cf, "source_elapsed", elapsed);
                addColumn(cf, "activity", event.getMessage());
                RowMutation mutation = new RowMutation(Tracing.TRACE_KS, state.sessionIdBytes);
                mutation.add(cf);
                StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
            }
        });
    }

    public void close()
    {
    }

    public boolean requiresLayout()
    {
        return false;
    }
}
