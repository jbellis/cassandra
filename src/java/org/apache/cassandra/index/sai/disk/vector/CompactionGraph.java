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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.BufferedRandomAccessWriter;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.Feature;
import io.github.jbellis.jvector.graph.disk.FeatureId;
import io.github.jbellis.jvector.graph.disk.InlineVectors;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.pq.VectorCompressor;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v3.CassandraDiskAnn;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.util.StringHelper;

public class CompactionGraph implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ConcurrentVectorValues vectorValues;
    private final GraphIndexBuilder builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ChronicleMap<VectorFloat<?>, VectorPostings<Integer>> postingsMap;
    private volatile boolean postingsOneToOne;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private final VectorSourceModel sourceModel;
    private final VectorCompressor<?> compressor;
    private final BufferedRandomAccessWriter indexOutput;
    private final OnDiskGraphIndexWriter writer;
    private long termsOffset;

    public CompactionGraph(IndexDescriptor descriptor, IndexContext context, IndexWriterConfig indexConfig, VectorCompressor<?> compressor, int size) throws IOException
    {
        var termComparator = context.getValidator();
        serializer = (VectorType.VectorSerializer) termComparator.getSerializer();
        vectorValues = new ConcurrentVectorValues(((VectorType<?>) termComparator).dimension);
        similarityFunction = indexConfig.getSimilarityFunction();
        sourceModel = indexConfig.getSourceModel();
        var tmpMapFile = File.createTempFile("postingsMap", null);
        postingsMap = ChronicleMapBuilder.of((Class<VectorFloat<?>>) (Class) VectorFloat.class, (Class<VectorPostings<Integer>>) (Class) VectorPostings.class)
                                         .entries(indexConfig.getMaximumNodeConnections())
                                         .averageKeySize(vectorValues.dimension() * Float.BYTES)
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + VectorPostings.bytesPerPosting())
                                         .createPersistedTo(tmpMapFile);
        postingsOneToOne = true;
        builder = new GraphIndexBuilder(vectorValues,
                                        similarityFunction,
                                        indexConfig.getMaximumNodeConnections(),
                                        indexConfig.getConstructionBeamWidth(),
                                        1.2f,
                                        1.2f);
        this.compressor = compressor;
        indexOutput = IndexFileUtils.instance.openRandomAccessOutput(descriptor.fileFor(IndexComponent.TERMS_DATA, context), true);
        termsOffset = indexOutput.getFilePointer();
        writer = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), indexOutput, getIdentityMapper())
                 .with(new InlineVectors(vectorValues.dimension()))
                 .build();
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
        indexOutput.close();
        postingsMap.close();
    }

    public int size()
    {
        return vectorValues.size();
    }

    public boolean isEmpty()
    {
        return postingsMap.values().stream().allMatch(VectorPostings::isEmpty);
    }

    /**
     * @return the incremental bytes ysed by adding the given vector to the index
     */
    public long add(ByteBuffer term, Integer key) throws IOException
    {
        assert term != null && term.remaining() != 0;

        var vector = vts.createFloatVector(serializer.deserializeFloatArray(term));
        // Validate the vector.  Since we are compacting, invalid vectors are ignored instead of failing the operation.
        try
        {
            VectorValidation.validateIndexable(vector, similarityFunction);
        }
        catch (InvalidRequestException e)
        {
            if (StorageService.instance.isInitialized())
                logger.trace("Ignoring invalid vector during index build against existing data: {}", (Object) e);
            else
                logger.trace("Ignoring invalid vector during commitlog replay: {}", (Object) e);
            return 0;
        }

        var bytesUsed = 0L;
        var postings = postingsMap.get(vector);
        // if the vector is already in the graph, all that happens is that the postings list is updated
        // otherwise, we add the vector in this order:
        // 1. to the postingsMap
        // 2. to the vectorValues
        // 3. to the graph
        // This way, concurrent searches of the graph won't see the vector until it's visible
        // in the other structures as well.
        if (postings == null)
        {
            postings = new VectorPostings<>(key);
            // since we are using ConcurrentSkipListMap, it is NOT correct to use computeIfAbsent here
            if (postingsMap.putIfAbsent(vector, postings) == null)
            {
                // we won the race to add the new entry; assign it an ordinal and add to the other structures
                var ordinal = nextOrdinal.getAndIncrement();
                postings.setOrdinal(ordinal);
                bytesUsed += RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
                bytesUsed += vectorValues.add(ordinal, vector);
                bytesUsed += VectorPostings.emptyBytesUsed() + VectorPostings.bytesPerPosting();
                bytesUsed += builder.addGraphNode(ordinal, vector);

                // write the new vector to our index
                synchronized (writer)
                {
                    writer.writeInline(ordinal, Feature.singleState(FeatureId.INLINE_VECTORS, new InlineVectors.State(vector)));
                }

                return bytesUsed;
            }
            else
            {
                postings = postingsMap.get(vector);
                postingsOneToOne = false;
            }
        }
        // postings list already exists, just add the new key (if it's not already in the list)
        if (postings.add(key))
        {
            bytesUsed += VectorPostings.bytesPerPosting();
        }

        return bytesUsed;
    }

    public SegmentMetadata.ComponentMetadataMap writeNeighbors(IndexDescriptor indexDescriptor, IndexContext indexContext, Set<Integer> deletedOrdinals) throws IOException
    {
        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal.get() == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                              nextOrdinal.get(), builder.getGraph().size());
        assert vectorValues.size() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                                vectorValues.size(), builder.getGraph().size());
        assert postingsMap.keySet().size() == vectorValues.size() : String.format("postings map entry count %d != vector count %d",
                                                                                  postingsMap.keySet().size(), vectorValues.size());
        logger.debug("Writing graph with {} rows and {} distinct vectors", postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), vectorValues.size());

        try (var pqOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.PQ, indexContext), true);
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext), true))
        {
            SAICodecUtils.writeHeader(pqOutput);
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(indexOutput));

            // compute and write PQ
            long pqOffset = pqOutput.getFilePointer();
            long pqPosition = writePQ(pqOutput.asSequentialWriter(), i -> i, indexContext);
            long pqLength = pqPosition - pqOffset;

            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<Integer>(postingsOneToOne, i -> i)
                                            .writePostings(postingsOutput.asSequentialWriter(),
                                                           vectorValues, postingsMap, deletedOrdinals);
            long postingsLength = postingsPosition - postingsOffset;

            // complete (internal clean up) and write the graph
            builder.cleanup();

            var start = System.nanoTime();
            writer.write(new EnumMap<>(FeatureId.class));
            SAICodecUtils.writeFooter(indexOutput, writer.checksum());
            logger.info("Writing graph took {}ms", (System.nanoTime() - start) / 1_000_000);
            long termsLength = indexOutput.getFilePointer() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();
            metadataMap.put(IndexComponent.TERMS_DATA, -1, termsOffset, termsLength, Map.of());
            metadataMap.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength, Map.of());
            Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(StringHelper.randomId())));
            metadataMap.put(IndexComponent.PQ, -1, pqOffset, pqLength, vectorConfigs);
            return metadataMap;
        }
    }

    private long writePQ(SequentialWriter writer, IntUnaryOperator reverseOrdinalMapper, IndexContext indexContext) throws IOException
    {
        var preferredCompression = sourceModel.compressionProvider.apply(vectorValues.dimension());

        // Build encoder and compress vectors
        VectorCompressor<?> compressor; // will be null if we can't compress
        Object encoded = null; // byte[][], or long[][]
        boolean containsUnitVectors;
        // limit the PQ computation and encoding to one index at a time -- goal during flush is to
        // evict from memory ASAP so better to do the PQ build (in parallel) one at a time
        synchronized (CompactionGraph.class)
        {
            assert !vectorValues.isValueShared();
            // encode (compress) the vectors to save
            if (compressor != null)
                encoded = compressVectors(reverseOrdinalMapper, compressor);

            containsUnitVectors = IntStream.range(0, vectorValues.size())
                                           .parallel()
                                           .mapToObj(vectorValues::vectorValue)
                                           .allMatch(v -> Math.abs(VectorUtil.dotProduct(v, v) - 1.0f) < 0.01);
        }

        // version and optional fields
        writer.writeInt(CassandraDiskAnn.PQ_MAGIC);
        writer.writeInt(1); // version
        writer.writeBoolean(containsUnitVectors);

        // write the compression type
        var actualType = compressor == null ? VectorCompression.CompressionType.NONE : preferredCompression.type;
        writer.writeByte(actualType.ordinal());
        if (actualType == VectorCompression.CompressionType.NONE)
            return writer.position();

        // save (outside the synchronized block, this is io-bound not CPU)
        CompressedVectors cv;
        if (compressor instanceof BinaryQuantization)
            cv = new BQVectors((BinaryQuantization) compressor, (long[][]) encoded);
        else
            cv = new PQVectors((ProductQuantization) compressor, (ByteSequence<?>[]) encoded);
        cv.write(writer);
        return writer.position();
    }

    public long ramBytesUsed()
    {
        return postingsBytesUsed() + vectorValues.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    private long postingsBytesUsed()
    {
        return postingsMap.values().stream().mapToLong(VectorPostings::ramBytesUsed).sum();
    }

    private long exactRamBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }

    private static OnDiskGraphIndexWriter.OrdinalMapper getIdentityMapper()
    {
        return new OnDiskGraphIndexWriter.OrdinalMapper()
        {
            public int oldToNew(int oldOrdinal)
            {
                return oldOrdinal;
            }

            public int newToOld(int newOrdinal)
            {
                return newOrdinal;
            }
        };
    }
}
