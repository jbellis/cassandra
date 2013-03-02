package org.apache.cassandra.service.paxos;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.FQRow;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public UUID mostRecentCommitted = UUIDGen.minTimeUUID(0);
    public UUID inProgressBallot = UUIDGen.minTimeUUID(0);
    public FQRow inProgressUpdates = null;

    public PrepareCallback(int targets)
    {
        super(targets);
    }

    public void response(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.debug("Prepare response {} from {}", response, message.from);

        promised &= response.promised;

        if (FBUtilities.timeComparator.compare(response.mostRecentCommitted, mostRecentCommitted) > 0)
            mostRecentCommitted = response.mostRecentCommitted;

        if (FBUtilities.timeComparator.compare(response.inProgressBallot, inProgressBallot) > 0)
        {
            inProgressBallot = response.inProgressBallot;
            inProgressUpdates = response.inProgressUpdates;
        }

        latch.countDown();
    }
}
