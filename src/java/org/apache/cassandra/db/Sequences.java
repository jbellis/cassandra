package org.apache.cassandra.db;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestTimeoutException;

import static org.apache.cassandra.cql3.QueryProcessor.process;

public class Sequences
{
    public static final String SEQUENCES_KS = "system_sequences";
    public static final String SEQUENCES_CF = "sequences";

    public static void create(String sequenceName, long start) throws RequestExecutionException
    {
        String cql = "INSERT INTO %s.%s (sequence_name, value) values ('%s', %s) IF NOT EXISTS";
        process(String.format(cql, SEQUENCES_KS, SEQUENCES_CF, safeName(sequenceName), start), ConsistencyLevel.ONE);
    }

    public static long next(String sequenceName) throws RequestExecutionException
    {
        String selectCql = "SELECT value FROM %s.%s WHERE sequence_name = '%s'";
        String updateCql = "UPDATE %s.%s SET value = %s WHERE sequence_name = '%s' IF value = %s";

        String formattedSelect = String.format(selectCql, SEQUENCES_KS, SEQUENCES_CF, safeName(sequenceName));
        outer:
        while (true)
        {
            long value;
            try
            {
                value = process(formattedSelect, ConsistencyLevel.SERIAL).one().getLong("value");
            }
            catch (RequestTimeoutException e)
            {
                continue;
            }

            while (true)
            {
                long newValue = value + 1;
                String formattedUpdate = String.format(updateCql, SEQUENCES_KS, SEQUENCES_CF, newValue, safeName(sequenceName), value);
                UntypedResultSet result = null;
                try
                {
                    result = process(formattedUpdate, ConsistencyLevel.ONE);
                }
                catch (RequestTimeoutException e)
                {
                    continue outer;
                }
                assert !result.isEmpty();
                UntypedResultSet.Row row = result.one();
                boolean applied = row.getBoolean("[applied]");
                if (applied)
                    return newValue;
                value = row.getLong("value");
            }
        }
    }

    private static String safeName(String sequenceName)
    {
        return sequenceName.replace("'", "''");
    }
}
