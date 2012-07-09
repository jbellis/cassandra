/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.EncodedDataInputStream;
import org.apache.cassandra.utils.vint.EncodedDataOutputStream;
import org.junit.Assert;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AbstractSerializationsTester extends SchemaLoader
{
    protected static final String CUR_VER = System.getProperty("cassandra.version", "1.2");
    protected static final Map<String, Integer> VERSION_MAP = new HashMap<String, Integer>()
    {{
        put("0.7", 1);
        put("1.0", 3);
        put("1.2", MessagingService.VERSION_12);
    }};

    // TODO ant doesn't pass this -D up to the test, so it's kind of useless
    protected static final boolean EXECUTE_WRITES = Boolean.getBoolean("cassandra.test-serialization-writes");
    private FileOutputStream fos;
    private FileInputStream fis;
    

    protected final int getVersion()
    {
        return VERSION_MAP.get(CUR_VER);
    }

    protected <T> void testSerializedSize(T obj, IVersionedSerializer<T> serializer) throws IOException
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        serializer.serialize(obj, FBUtilities.getEncodedOutput(out, getVersion()), getVersion());
        Assert.assertEquals(serializer.serializedSize(obj, getVersion()), out.toByteArray().length);
    }

    protected DataInput getInput(String name) throws IOException
    {
        return new EncodedDataInputStream(getRawInput(name));
    }

    protected DataInput getRawInput(String name) throws IOException
    {
        File f = new File("test/data/serialization/" + CUR_VER + "/" + name);
        assert f.exists() : f.getPath();
        fis = new FileInputStream(f);
        return new DataInputStream(fis);
    }

    protected DataOutput getOutput(String name) throws IOException
    {
        return new EncodedDataOutputStream(getRawOutput(name));
    }

    protected DataOutput getRawOutput(String name) throws IOException
    {
        File f = new File("test/data/serialization/" + CUR_VER + "/" + name);
        f.getParentFile().mkdirs();
        fos = new FileOutputStream(f);
        return new DataOutputStream(fos);
    }

    public void close()
    {
        FileUtils.closeQuietly(fis);
        fis = null;
        FileUtils.closeQuietly(fos);
        fos = null;
    }
}
