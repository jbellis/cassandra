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
package org.apache.cassandra.db;

public abstract class TypeSizes
{
    public static final TypeSizes NATIVE = new NativeDBTypeSizes();
    public static final TypeSizes VINT = new VIntEncodedDBTypeSizes();

    private static final int BOOL_SIZE = 1;
    private static final int SHORT_SIZE = 2;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;

    public abstract int sizeof(boolean value);
    public abstract int sizeof(short value);
    public abstract int sizeof(int value);
    public abstract int sizeof(long value);

    public static class NativeDBTypeSizes extends TypeSizes
    {
        public int sizeof(boolean value)
        {
            return BOOL_SIZE;
        }

        public int sizeof(short value)
        {
            return SHORT_SIZE;
        }

        public int sizeof(int value)
        {
            return INT_SIZE;
        }

        public int sizeof(long value)
        {
            return LONG_SIZE;
        }
    }

    public static class VIntEncodedDBTypeSizes extends TypeSizes
    {
        private static final int BOOL_SIZE = 1;

        public int sizeofVInt(long i)
        {
            if (i >= -112 && i <= 127)
                return 1;

            int size = 0;
            int len = -112;
            if (i < 0)
            {
                i ^= -1L; // take one's complement'
                len = -120;
            }
            long tmp = i;
            while (tmp != 0)
            {
                tmp = tmp >> 8;
                len--;
            }
            size++;
            len = (len < -120) ? -(len + 120) : -(len + 112);
            size += len;
            return size;
        }

        public int sizeof(long i)
        {
            return sizeofVInt(i);
        }

        public int sizeof(boolean i)
        {
            return BOOL_SIZE;
        }

        public int sizeof(short i)
        {
            return sizeofVInt(i);
        }

        public int sizeof(int i)
        {
            return sizeofVInt(i);
        }
    }
}
