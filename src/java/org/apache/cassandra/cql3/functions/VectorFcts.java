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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class VectorFcts
{
    private static boolean isFloatVector(AbstractType<?> type)
    {
        type = type.unwrap();
        return type instanceof VectorType && ((VectorType<?>) type).getElementsType() == FloatType.instance;
    }

    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new FunctionFactory("similarity_cosine", FunctionParameter.anyType(true), FunctionParameter.anyType(true))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (argTypes.size() != 2)
                    return null;
                AbstractType<?> outputType = receiverType == null ? argTypes.get(0) : receiverType;
                if (!(isFloatVector(outputType) && argTypes.stream().allMatch(VectorFcts::isFloatVector)))
                    return null;
                if (!argTypes.stream().allMatch(t -> t.equals(outputType)))
                    return null;
                VectorType<Float> type = (VectorType<Float>) outputType;
                return makeSimilarityCosine(name.name, type);
            }
        });
    }

    private static NativeFunction makeSimilarityCosine(String name, VectorType<Float> type)
    {
        return new NativeScalarFunction(name, type, type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                return parameters.get(0);
            }
        };
    }
}
