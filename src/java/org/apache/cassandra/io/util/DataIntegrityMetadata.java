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
package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.PureJavaCrc32;

public class DataIntegrityMetadata
{
    public static ChecksumValidator checksumValidator(Descriptor desc) throws IOException
    {
        return new ChecksumValidator(desc);
    }

    public static class ChecksumValidator implements Closeable
    {
        private final Checksum checksum;
        private final RandomAccessReader reader;
        private final Descriptor descriptor;
        public final int chunkSize;

        public ChecksumValidator(Descriptor descriptor) throws IOException
        {
            this.descriptor = descriptor;
            checksum = descriptor.version.hasAllAdlerChecksums ? new Adler32() : new PureJavaCrc32();
            reader = RandomAccessReader.open(new File(descriptor.filenameFor(Component.CRC)));
            chunkSize = reader.readInt();
        }

        public void seek(long offset)
        {
            long start = chunkStart(offset);
            reader.seek(((start / chunkSize) * 4L) + 4); // 8 byte checksum per
                                                         // chunk + 4 byte
                                                         // header/chunkLength
        }

        public long chunkStart(long offset)
        {
            long startChunk = offset / chunkSize;
            return startChunk * chunkSize;
        }

        public void validate(byte[] bytes, int start, int end) throws IOException
        {
            checksum.update(bytes, start, end);
            int current = (int) checksum.getValue();
            checksum.reset();
            int actual = reader.readInt();
            if (current != actual)
                throw new IOException("Corrupted SSTable : " + descriptor.filenameFor(Component.DATA));
        }

        public void close()
        {
            reader.close();
        }
    }

    public static class ChecksumWriter
    {
        private final Checksum checksum = new Adler32();
        private final MessageDigest digest;
        private final DataOutputStream incrementalOut;

        public ChecksumWriter(DataOutputStream incrementalOut)
        {
            this.incrementalOut = incrementalOut;
            try
            {
                digest = MessageDigest.getInstance("SHA-1");
            }
            catch (NoSuchAlgorithmException e)
            {
                // SHA-1 is standard in java 6
                throw new RuntimeException(e);
            }
        }

        public void writeChunkSize(int length)
        {
            try
            {
                incrementalOut.writeInt(length);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public void append(byte[] buffer, int start, int end)
        {
            try
            {
                checksum.update(buffer, start, end);
                incrementalOut.writeInt((int) checksum.getValue());
                checksum.reset();
                digest.update(buffer, start, end);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public void writeFullChecksum(Descriptor descriptor)
        {
            byte[] bytes = digest.digest();
            SequentialWriter out = SequentialWriter.open(new File(descriptor.filenameFor(Component.DIGEST)), true);
            // Writting output compatible with sha1sum
            Descriptor newdesc = descriptor.asTemporary(false);
            String[] tmp = newdesc.filenameFor(Component.DATA).split(Pattern.quote(File.separator));
            String dataFileName = tmp[tmp.length - 1];
            try
            {
                out.write(String.format("%s  %s", Hex.bytesToHex(bytes), dataFileName).getBytes());
            }
            catch (ClosedChannelException e)
            {
                throw new AssertionError(); // can't happen.
            }
            finally
            {
                FileUtils.closeQuietly(out);
            }
        }
    }
}
