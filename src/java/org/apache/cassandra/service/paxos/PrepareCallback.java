package org.apache.cassandra.service.paxos;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public UUID mostRecentCommitted = UUIDGen.minTimeUUID(0);
    public UUID inProgressBallot = UUIDGen.minTimeUUID(0);
    public Row inProgressUpdates = null;

    private SortedMap<UUID, AtomicInteger> sharedCommits = new TreeMap<UUID, AtomicInteger>(FBUtilities.timeComparator);

    public PrepareCallback(int targets)
    {
        super(targets);
    }

    public synchronized void response(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.debug("Prepare response {} from {}", response, message.from);

        promised &= response.promised;

        AtomicInteger mrc = sharedCommits.get(response.mostRecentCommitted);
        if (mrc == null)
        {
            mrc = new AtomicInteger(0);
            sharedCommits.put(response.mostRecentCommitted, mrc);
        }
        mrc.incrementAndGet();

        if (response.mostRecentCommitted.timestamp() > mostRecentCommitted.timestamp())
            mostRecentCommitted = response.mostRecentCommitted;

        if (response.inProgressBallot.timestamp() > inProgressBallot.timestamp())
        {
            inProgressBallot = response.inProgressBallot;
            inProgressUpdates = response.inProgressUpdates;
        }

        latch.countDown();
    }

    public boolean mostRecentCommitHasQuorum(int required)
    {
        return sharedCommits.get(sharedCommits.lastKey()).get() >= required;
    }
}
