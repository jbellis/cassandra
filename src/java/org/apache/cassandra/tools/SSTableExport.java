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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.disk.hnsw.OnDiskOrdinalsMap;
import org.apache.cassandra.index.sai.disk.hnsw.OnDiskVectors;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.transport.ProtocolVersion;
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

    private static final String KEY_OPTION = "k";
    private static final String DEBUG_OUTPUT_OPTION = "d";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String ENUMERATE_KEYS_OPTION = "e";
    private static final String RAW_TIMESTAMPS = "t";
    private static final String PARTITION_JSON_LINES = "l";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static
    {
        DatabaseDescriptor.clientInitialization();

        Option optKey = new Option(KEY_OPTION, true, "List of included partition keys");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(500);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "List of excluded partition keys");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(500);
        options.addOption(excludeKey);

        Option optEnumerate = new Option(ENUMERATE_KEYS_OPTION, false, "enumerate partition keys only");
        options.addOption(optEnumerate);

        Option debugOutput = new Option(DEBUG_OUTPUT_OPTION, false, "CQL row per line internal representation");
        options.addOption(debugOutput);

        Option rawTimestamps = new Option(RAW_TIMESTAMPS, false, "Print raw timestamps instead of iso8601 date strings");
        options.addOption(rawTimestamps);

        Option partitionJsonLines= new Option(PARTITION_JSON_LINES, false, "Output json lines, by partition");
        options.addOption(partitionJsonLines);
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
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        String[] keys = cmd.getOptionValues(KEY_OPTION);
        HashSet<String> excludes = new HashSet<>(Arrays.asList(
                cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                        ? new String[0]
                        : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));

        if (cmd.getArgs().length != 1)
        {
            String msg = "You must supply exactly one sstable";
            if (cmd.getArgs().length == 0 && (keys != null && keys.length > 0 || !excludes.isEmpty()))
                msg += ", which should be before the -k/-x options so it's not interpreted as a partition key.";

            System.err.println(msg);
            printUsage();
            System.exit(1);
        }
        String ssTableFileName = new File(cmd.getArgs()[0]).absolutePath();

        if (!new File(ssTableFileName).exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }
        System.out.println("Opening " + ssTableFileName);
        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        try
        {
            TableMetadata metadata = Util.metadataFromSSTable(desc);
            SSTableReader sstable = desc.getFormat().getReaderFactory().openNoValidation(desc, TableMetadataRef.forOfflineTools(metadata));

            var vectorFile = new File(sstable.getDescriptor().baseFilename() + "-SAI+ba+ann_index+Vector.db");
            var offsetsFile = new File(sstable.getDescriptor().baseFilename() + "-SAI+ba+ann_index+PostingLists.db");
            FileHandle vectorsHandle = new FileHandle.Builder(vectorFile).mmapped(true).complete();
            var ordinalSegmentOffsets = new OrdinalsMapOffsetReconstructor(offsetsFile.toJavaIOFile());
            AtomicLong vectorsOffset = new AtomicLong();
            var vectors = new AtomicReference<>(new OnDiskVectors(vectorsHandle, vectorsOffset.get()));
            FileHandle ordinalsHandle = new FileHandle.Builder(offsetsFile).mmapped(true).complete();
            List<Long> segmentOffsets = ordinalSegmentOffsets.getSegmentOffsets();
            var ordinals = new AtomicReference<>(new OnDiskOrdinalsMap(ordinalsHandle, 0, segmentOffsets.get(1)).getOrdinalsView());
//            assert ordinalSegmentOffsets.getVectorCount() == vectors.get().size() : "Vector count mismatch " + ordinalSegmentOffsets.getVectorCount() + " != " + vectors.get().size();

            final ISSTableScanner currentScanner;
            currentScanner = sstable.getScanner();
            Stream<UnfilteredRowIterator> partitions = Util.iterToStream(currentScanner).filter(i ->
                excludes.isEmpty() || !excludes.contains(metadata.partitionKeyType.getString(i.partitionKey().getKey()))
            );
            AtomicLong position = new AtomicLong();
            AtomicInteger rowId = new AtomicInteger();
            AtomicInteger segment = new AtomicInteger();
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
                            if (rowId.get() >= ordinalSegmentOffsets.getLastRowId()) {
                                vectorsOffset.addAndGet(8L + ordinalSegmentOffsets.getVectorCount() * 4L * v1.length);
                                vectors.set(new OnDiskVectors(vectorsHandle, vectorsOffset.get()));
                                segment.incrementAndGet();
                                var sStart = segmentOffsets.get(segment.get());
                                var sLength = segmentOffsets.get(segment.get() + 1) - sStart;
                                ordinals.set(new OnDiskOrdinalsMap(ordinalsHandle, sStart, sLength).getOrdinalsView());
                                rowId.set(0);
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

        System.exit(0);
    }

    private static void printUsage()
    {
        String usage = String.format("sstabledump <sstable file path> <options>%n");
        String header = "Dump contents of given SSTable to standard output in JSON format.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
