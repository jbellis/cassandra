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
package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.disk.hnsw.OnDiskVectors;
import org.apache.cassandra.index.sai.disk.hnsw.pq.ProductQuantization;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Export SSTables to JSON format.
 */
public class SSTableExport
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file, export the contents of the SSTable to JSON.
     *
     * @param args
     *            command lines arguments
     * @throws ConfigurationException
     *             on configuration failure (wrong params given)
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws ConfigurationException
    {
        java.io.File ssTableDirectory = new java.io.File(new File("/home/jonathan/Projects/cassandra/data/data/wikipedia/pages-750c3f2032cf11eeae989d948bdd7066").absolutePath());
        Arrays.stream(ssTableDirectory.listFiles((dir, name) -> name.endsWith("+Vector.db")))
              .forEach(SSTableExport::addPQ);
    }

    /** writes the first segment of vectors in fvec format */
    private static void addPQ(java.io.File ssTableFileName)
    {
        Descriptor desc = Descriptor.fromFilename(new File(ssTableFileName));
        try
        {
            inner(desc);
        }
        catch (Throwable e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
//            System.err.println("Error reading " + ssTableFileName);
            throw new RuntimeException(e);
        }
    }

    private static void inner(Descriptor desc) throws IOException
    {
        TableMetadata metadata = Util.metadataFromSSTable(desc);
        SSTableReader sstable = desc.getFormat().getReaderFactory().openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));

        var vectorFile = new File(sstable.getDescriptor().baseFilename() + "-SAI+ba+ann_index+Vector.db");
        FileHandle vectorsHandle = new FileHandle.Builder(vectorFile).mmapped(true).complete();
        int offset = 0;
        while (true) {
            var odv = new OnDiskVectors(vectorsHandle, offset);
            var vectors = IntStream.range(0, odv.size()).mapToObj(i -> {
                try
                {
                    var v = new float[odv.dimension()];
                    System.arraycopy(odv.vectorValue(i), 0, v, 0, odv.dimension());
                    return v;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());

            // train PQ
            int M = odv.dimension() / 2;
            var pq = new ProductQuantization(vectors, M, false);
            var vectorsOut = new java.io.File(sstable.getDescriptor().baseFilename() + "-SAI+ba+ann_index+PQ.db");
            var encoded = vectors.stream().parallel().map(pq::encode).collect(Collectors.toList());
            try (var vectorsWriter = new java.io.BufferedOutputStream(new java.io.FileOutputStream(vectorsOut)))
            {
                vectorsWriter.write(encoded.size());
                vectorsWriter.write(encoded.get(0).length);
                for (var a: encoded) {
                    vectorsWriter.write(a);
                }
            }

            // two ints, plus all the vectors we read
            offset += 4 + 4 + (4 * vectors.size() * odv.dimension());
        }
    }
}
