/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.SemanticVersion;

/**
 * A container for per-client, thread-local state that Avro/Thrift threads must hold.
 * TODO: Kill thrift exceptions
 */
public class ClientState
{
    private static final int MAX_CACHE_PREPARED = 10000;    // Enough to keep buggy clients from OOM'ing us
    private static final Logger logger = LoggerFactory.getLogger(ClientState.class);
    public static final SemanticVersion DEFAULT_CQL_VERSION = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;

    // Current user for the session
    private AuthenticatedUser user;
    private String keyspace;
    private double traceProbability = 0.0;
    private int maxTracedSessions = 0;
    private int currentTracedSessions = 0;
    private UUID preparedTracingSession;
    private Random random = new Random(1234L);

    // Reusable array for authorization
    private final List<Object> resource = new ArrayList<Object>();
    private SemanticVersion cqlVersion = DEFAULT_CQL_VERSION;

    // An LRU map of prepared statements
    private final Map<Integer, CQLStatement> prepared = new LinkedHashMap<Integer, CQLStatement>(16, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<Integer, CQLStatement> eldest) {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    private final Map<Integer, org.apache.cassandra.cql3.CQLStatement> cql3Prepared = new LinkedHashMap<Integer, org.apache.cassandra.cql3.CQLStatement>(16, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<Integer, org.apache.cassandra.cql3.CQLStatement> eldest) {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    private long clock;

    /**
     * Construct a new, empty ClientState: can be reused after logout() or reset().
     */
    public ClientState()
    {
        reset();
    }

    public Map<Integer, CQLStatement> getPrepared()
    {
        return prepared;
    }

    public Map<Integer, org.apache.cassandra.cql3.CQLStatement> getCQL3Prepared()
    {
        return cql3Prepared;
    }

    public String getRawKeyspace()
    {
        return keyspace;
    }

    public String getKeyspace() throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("no keyspace has been specified");
        return keyspace;
    }

    public void setKeyspace(String ks) throws InvalidRequestException
    {
        if (Schema.instance.getKSMetaData(ks) == null)
            throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
        keyspace = ks;
    }

    public boolean traceNextQuery()
    {
        if (preparedTracingSession != null)
        {
            return true;
        }

        if (random.nextDouble() < traceProbability
                && ((currentTracedSessions < maxTracedSessions) || (maxTracedSessions == -1)))
        {
            currentTracedSessions++;
            return true;
        }
        return false;
    }

    public void enableTracing(double probability, int number)
    {
        this.traceProbability = probability;
        this.maxTracedSessions = number;
        if (traceProbability > 0.0 && maxTracedSessions > 0)
        {
            if (logger.isDebugEnabled())
                logger.debug("tracing enabled. Tracing a total of: " + number + " queries with a probability of "
                        + probability);
        }
    }

    public void disableTracing()
    {
        this.traceProbability = 0.0;
        this.maxTracedSessions = 0;
        this.preparedTracingSession = null;
    }

    public void prepareTracingSession(UUID sessionId)
    {
        this.preparedTracingSession = sessionId;
    }

    public UUID getPreparedSessionIdAndReset()
    {
        UUID preparedTracingSession = this.preparedTracingSession;
        this.preparedTracingSession = null;
        return preparedTracingSession;
    }

    public boolean isPreparedTracingSession()
    {
        return this.preparedTracingSession != null;
    }

    public String getSchedulingValue()
    {
        switch(DatabaseDescriptor.getRequestSchedulerId())
        {
            case keyspace: return keyspace;
        }
        return "default";
    }

    /**
     * Attempts to login this client with the given credentials map.
     */
    public void login(Map<? extends CharSequence,? extends CharSequence> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);
        if (logger.isDebugEnabled())
            logger.debug("logged in: {}", user);
        this.user = user;
    }

    public void logout()
    {
        if (logger.isDebugEnabled())
            logger.debug("logged out: {}", user);
        reset();
    }

    private void resourceClear()
    {
        resource.clear();
        resource.add(Resources.ROOT);
        resource.add(Resources.KEYSPACES);
    }

    public void reset()
    {
        user = DatabaseDescriptor.getAuthenticator().defaultUser();
        keyspace = null;
        maxTracedSessions = 0;
        traceProbability = 0;
        preparedTracingSession = null;
        resourceClear();
        prepared.clear();
        cql3Prepared.clear();
    }

    /**
     * Confirms that the client thread has the given Permission for the Keyspace list.
     */
    public void hasKeyspaceSchemaAccess(Permission perm) throws InvalidRequestException
    {
        validateLogin();

        resourceClear();
        Set<Permission> perms = DatabaseDescriptor.getAuthority().authorize(user, resource);

        hasAccess(user, perms, perm, resource);
    }

    public void hasColumnFamilySchemaAccess(Permission perm) throws InvalidRequestException
    {
        hasColumnFamilySchemaAccess(keyspace, perm);
    }

    /**
     * Confirms that the client thread has the given Permission for the ColumnFamily list of
     * the provided keyspace.
     */
    public void hasColumnFamilySchemaAccess(String keyspace, Permission perm) throws InvalidRequestException
    {
        validateLogin();
        validateKeyspace(keyspace);

        // hardcode disallowing messing with system keyspace
        if (keyspace.equalsIgnoreCase(Table.SYSTEM_TABLE) && perm == Permission.WRITE)
            throw new InvalidRequestException("system keyspace is not user-modifiable");

        resourceClear();
        resource.add(keyspace);
        Set<Permission> perms = DatabaseDescriptor.getAuthority().authorize(user, resource);

        hasAccess(user, perms, perm, resource);
    }

    /**
     * Confirms that the client thread has the given Permission in the context of the given
     * ColumnFamily and the current keyspace.
     */
    public void hasColumnFamilyAccess(String columnFamily, Permission perm) throws InvalidRequestException
    {
        hasColumnFamilyAccess(keyspace, columnFamily, perm);
    }

    public void hasColumnFamilyAccess(String keyspace, String columnFamily, Permission perm) throws InvalidRequestException
    {
        validateLogin();
        validateKeyspace(keyspace);

        resourceClear();
        resource.add(keyspace);
        resource.add(columnFamily);
        Set<Permission> perms = DatabaseDescriptor.getAuthority().authorize(user, resource);

        hasAccess(user, perms, perm, resource);
    }

    public boolean isLogged()
    {
        return user != null;
    }

    private void validateLogin() throws InvalidRequestException
    {
        if (user == null)
            throw new InvalidRequestException("You have not logged in");
    }

    private static void validateKeyspace(String keyspace) throws InvalidRequestException
    {
        if (keyspace == null)
            throw new InvalidRequestException("You have not set a keyspace for this session");
    }

    private static void hasAccess(AuthenticatedUser user, Set<Permission> perms, Permission perm, List<Object> resource) throws InvalidRequestException
    {
        if (perms.contains(perm))
            return;
        throw new InvalidRequestException(String.format("%s does not have permission %s for %s",
                                                        user,
                                                        perm,
                                                        Resources.toString(resource)));
    }

    /**
     * This clock guarantees that updates from a given client will be ordered in the sequence seen,
     * even if multiple updates happen in the same millisecond.  This can be useful when a client
     * wants to perform multiple updates to a single column.
     */
    public long getTimestamp()
    {
        long current = System.currentTimeMillis() * 1000;
        clock = clock >= current ? clock + 1 : current;
        return clock;
    }

    public void setCQLVersion(String str) throws InvalidRequestException
    {
        SemanticVersion version;
        try
        {
            version = new SemanticVersion(str);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        SemanticVersion cql = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;
        SemanticVersion cql3 = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

        if (version.isSupportedBy(cql))
            cqlVersion = cql;
        else if (version.isSupportedBy(cql3))
            cqlVersion = cql3;
        else
            throw new InvalidRequestException(String.format("Provided version %s is not supported by this server (supported: %s)",
                                                            version,
                                                            StringUtils.join(getCQLSupportedVersion(), ", ")));
    }

    public SemanticVersion getCQLVersion()
    {
        return cqlVersion;
    }

    public static SemanticVersion[] getCQLSupportedVersion()
    {
        SemanticVersion cql = org.apache.cassandra.cql.QueryProcessor.CQL_VERSION;
        SemanticVersion cql3 = org.apache.cassandra.cql3.QueryProcessor.CQL_VERSION;

        return new SemanticVersion[]{ cql, cql3 };
    }
}
