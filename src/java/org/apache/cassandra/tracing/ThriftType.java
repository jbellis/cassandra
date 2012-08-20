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
package org.apache.cassandra.tracing;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.config.ConfigurationException;

import com.google.common.base.Throwables;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 * A type for thrift objects that is able to seemslessly serialize and deserialize them as any other type. As other
 * types this type can also be built from a string.
 */
public class ThriftType extends AbstractType<TBase<?, ?>>
{

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ThriftType getInstance(TypeParser parser) throws ConfigurationException
    {
        parser.readNextChar();
        Character xar = null;
        StringBuilder builder = new StringBuilder();
        while ((xar = parser.readNextChar()) != ')')
        {
            builder.append(xar);
        }
        try
        {
            return new ThriftType((Class<? extends TBase>) Class.forName(builder.toString()));
        }
        catch (ClassNotFoundException e)
        {
            throw new ConfigurationException("cannot find thrift object class: " + builder.toString());
        }
    }

    @SuppressWarnings("rawtypes")
    public static ThriftType getInstance(Class<? extends TBase> thriftObjectClass)
    {
        return new ThriftType(thriftObjectClass);
    }

    private TSerializer serializer;
    private TDeserializer deserializer;
    @SuppressWarnings("rawtypes")
    private Class<? extends TBase> objectClass;

    @SuppressWarnings("rawtypes")
    ThriftType(Class<? extends TBase> objectClass)
    {
        this.objectClass = objectClass;
        this.serializer = new TSerializer();
        this.deserializer = new TDeserializer();
    }

    @Override
    public TBase<?, ?> compose(ByteBuffer bytes)
    {
        TBase<?, ?> obj;
        try
        {
            obj = this.objectClass.newInstance();
            this.deserializer.deserialize(obj, bytes.array());
            return obj;
        }
        catch (Exception e)
        {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ByteBuffer decompose(TBase<?, ?> value)
    {
        try
        {
            return ByteBuffer.wrap(this.serializer.serialize(value));
        }
        catch (TException e)
        {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ThriftType other = (ThriftType) obj;
        if (objectClass == null)
        {
            if (other.objectClass != null)
                return false;
        }
        else if (!objectClass.equals(other.objectClass))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "(" + objectClass.getName() + ")";
    }

    @Override
    public int compare(ByteBuffer arg0, ByteBuffer arg1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        throw new UnsupportedOperationException();
    }

}
