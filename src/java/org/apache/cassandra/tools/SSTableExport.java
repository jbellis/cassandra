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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnHeapHnsw;
import org.apache.cassandra.index.sai.disk.hnsw.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.hnsw.OnDiskVectors;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
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
        java.io.File ssTableDirectory = new java.io.File(new File(args[0]).absolutePath());
        if (!ssTableDirectory.isDirectory())
        {
            System.err.println("Path is not a directory");
            System.exit(1);
        }

//        Arrays.stream(ssTableDirectory.toJavaIOFile().listFiles((dir, name) -> name.endsWith("-Data.db")))
//              .parallel().forEach(SSTableExport::processSSTable);
        Arrays.stream(ssTableDirectory.listFiles((dir, name) -> name.endsWith("+Vector.db")))
              .parallel().forEach(SSTableExport::processVectors);
        System.exit(0);
    }

    private static void processVectors(java.io.File vectorsFileName)
    {
        var vectorFile = new File(vectorsFileName);
        try (FileHandle vectorsHandle = new FileHandle.Builder(vectorFile).mmapped(true).complete())
        {
            long vectorsOffset = 0;
            var vectors = new OnDiskVectors(vectorsHandle, vectorsOffset);
            int bad = 0;
            while (vectorsOffset < vectorFile.length())
            {
                for (int i = 0; i < vectors.size(); i++)
                {
                    float[] v = vectors.vectorValue(i);
                    try
                    {
                        CassandraOnHeapHnsw.checkInBounds(v);
                    } catch (IllegalArgumentException e) {
                        bad++;
                    }
                }
                vectorsOffset += 8L + vectors.size() * 4L * vectors.dimension();
            }
            System.out.printf("%d bad vectors in %s%n", bad, vectorsFileName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void processSSTable(java.io.File ssTableFileName)
    {
        Descriptor desc = Descriptor.fromFilename(new File(ssTableFileName));
        try
        {
            TableMetadata metadata = Util.metadataFromSSTable(desc);
            SSTableReader sstable = desc.getFormat().getReaderFactory().openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));

            var vectorFile = new File(sstable.getDescriptor().baseFilename() + "-SAI+ba+ann_index+Vector.db");
            var offsetsFile = new File(sstable.getDescriptor().baseFilename() + "-SAI+ba+ann_index+PostingLists.db");
            FileHandle vectorsHandle = new FileHandle.Builder(vectorFile).mmapped(true).complete();
            var ordinalSegments = new OrdinalsMapOffsetReconstructor(offsetsFile.toJavaIOFile()).segments;
            AtomicLong vectorsOffset = new AtomicLong();
            var vectors = new AtomicReference<>(new OnDiskVectors(vectorsHandle, vectorsOffset.get()));
            FileHandle ordinalsHandle = new FileHandle.Builder(offsetsFile).mmapped(true).complete();
            var ordinals = new AtomicReference<>(new OnDiskOrdinalsMap(ordinalsHandle, 0, ordinalSegments.get(1).offset).getOrdinalsView());
            assert ordinalSegments.get(0).vectorCount == vectors.get().size() : "Vector count mismatch " + ordinalSegments.get(0).vectorCount + " != " + vectors.get().size();

            final ISSTableScanner currentScanner;
            currentScanner = sstable.getScanner();
            Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner);
            AtomicLong position = new AtomicLong();
            AtomicInteger rowId = new AtomicInteger();
            AtomicInteger segmentIndex = new AtomicInteger();
            partitions.forEach(partition ->
            {
                position.set(currentScanner.getCurrentPosition());
                partition.forEachRemaining(row ->
                {
                    for (var cd : (AbstractRow) row) {
                        var type = cd.column().type;
                        var cell = (Cell<?>) cd;
                        if (type instanceof VectorType)
                        {
                            if (rowId.get() > ordinalSegments.get(segmentIndex.get()).lastRowId) {
                                vectorsOffset.addAndGet(8L + ordinalSegments.get(segmentIndex.get()).vectorCount * 4L * vectors.get().dimension());
                                vectors.set(new OnDiskVectors(vectorsHandle, vectorsOffset.get()));
                                segmentIndex.incrementAndGet();
                                var sStart = ordinalSegments.get(segmentIndex.get()).offset;
                                if (sStart < 0) {
                                    throw new RuntimeException("Row " + rowId.get() + " out of bounds but no more segments");
                                }
                                var sLength = ordinalSegments.get(segmentIndex.get() + 1).offset - sStart;
                                ordinals.set(new OnDiskOrdinalsMap(ordinalsHandle, sStart, sLength).getOrdinalsView());
                                rowId.set(0);
                            }

                            float[] v1 = ((VectorType<?>) type).composeAsFloat(cell.buffer());
                            float[] v2;
                            try
                            {
                                v2 = vectors.get().vectorValue(ordinals.get().getOrdinalForRowId(rowId.get()));
                            }
                            catch (IOException e)
                            {
                                throw new RuntimeException(e);
                            }
                            if (!Arrays.equals(v1, v2)) {
                                System.out.printf("Row %d mismatch%n", rowId.get());
                            }
                            break;
                        }
                    }
                    rowId.incrementAndGet();
                });
            });
            System.out.println("Scanned " + rowId.get() + " rows");
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }
    }
}
