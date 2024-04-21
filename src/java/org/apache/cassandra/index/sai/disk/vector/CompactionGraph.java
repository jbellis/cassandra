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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.Feature;
import io.github.jbellis.jvector.graph.disk.FeatureId;
import io.github.jbellis.jvector.graph.disk.InlineVectorValues;
import io.github.jbellis.jvector.graph.disk.InlineVectors;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.ArrayByteSequence;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
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
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ObjectSizes;

public class CompactionGraph implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final GraphIndexBuilder builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ChronicleMap<VectorFloat<?>, VectorPostings<Integer>> postingsMap;
    private final ChronicleMap<Integer, VectorFloat<?>> vectorsByOrdinal;
    private final InlineVectorValues inlineVectors;
    private final PQVectors pqVectors;
    private final ArrayList<ByteSequence<?>> pqVectorsList;
    private final IndexDescriptor descriptor;
    private final IndexContext context;
    private volatile boolean postingsOneToOne;
    private volatile int nextOrdinal = 0;
    private final VectorSourceModel sourceModel;
    private final ProductQuantization compressor;
    private final OnDiskGraphIndexWriter writer;
    private final long termsOffset;

    public CompactionGraph(IndexDescriptor descriptor, IndexContext context, ProductQuantization compressor, long keyCount) throws IOException
    {
        this.descriptor = descriptor;
        this.context = context;
        var indexConfig = context.getIndexWriterConfig();
        var termComparator = context.getValidator();
        int dimension = ((VectorType<?>) termComparator).dimension;

        // VSTODO keyCount is partitions, we need to push rows in here instead
        int estimatedSize = keyCount > 10_000_000 ? 10_000_000 : (int) keyCount;

        serializer = (VectorType.VectorSerializer) termComparator.getSerializer();
        similarityFunction = indexConfig.getSimilarityFunction();
        sourceModel = indexConfig.getSourceModel();
        postingsMap = ChronicleMapBuilder.of((Class<VectorFloat<?>>) (Class) VectorFloat.class, (Class<VectorPostings<Integer>>) (Class) VectorPostings.class)
                                         .entries(estimatedSize)
                                         .averageKeySize(dimension * Float.BYTES)
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + Integer.BYTES)
                                         .createPersistedTo(File.createTempFile("postingsMap", null));
        vectorsByOrdinal = ChronicleMapBuilder.of((Class<Integer>) (Class) Integer.class, (Class<VectorFloat<?>>) (Class) VectorPostings.class)
                                         .entries(estimatedSize)
                                         .averageValueSize(dimension * Float.BYTES)
                                         .createPersistedTo(File.createTempFile("vectorsByOrdinal", null));
        postingsOneToOne = true;
        this.compressor = compressor;
        builder = new GraphIndexBuilder(null,
                                        dimension,
                                        indexConfig.getMaximumNodeConnections(),
                                        indexConfig.getConstructionBeamWidth(),
                                        1.2f,
                                        dimension > 3 ? 1.2f : 1.4f,
                                        PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());

        var indexFile = descriptor.fileFor(IndexComponent.TERMS_DATA, context);
        termsOffset = (indexFile.exists() ? indexFile.length() : 0)
                      + SAICodecUtils.headerSize();
        writer = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), indexFile.toPath())
                 .withStartOffset(termsOffset)
                 .with(new InlineVectors(dimension))
                 .build();
        SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(writer.getOutput()));
        inlineVectors = new InlineVectorValues(dimension, writer);
        pqVectorsList = new ArrayList<>(estimatedSize);
        pqVectors = new PQVectors(compressor, pqVectorsList);
        // VSTODO add LVQ
        builder.setBuildScoreProvider(BuildScoreProvider.pqBuildScoreProvider(similarityFunction, inlineVectors, pqVectors));
    }

    @Override
    public void close() throws IOException
    {
        inlineVectors.close();
        writer.close();
        postingsMap.close();
    }

    public int size()
    {
        return builder.getGraph().size();
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
            if (postingsMap.putIfAbsent(vector, postings) == null)
            {
                // we won the race to add the new entry; assign it an ordinal and add to the other structures
                // synchronized so we can ensure that the ordering is consistent across the graph,
                // the inline vectors, and the PQ vectors
                synchronized (writer)
                {
                    var ordinal = nextOrdinal++;
                    vectorsByOrdinal.put(ordinal, vector);
                    postings.setOrdinal(ordinal);

                    var encoded = (ArrayByteSequence) compressor.encode(vector);
                    bytesUsed += RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
                    bytesUsed += encoded.get().length;
                    bytesUsed += VectorPostings.emptyBytesUsed() + VectorPostings.bytesPerPosting();
                    bytesUsed += builder.addGraphNode(ordinal, vector);

                    writer.writeInline(ordinal, Feature.singleState(FeatureId.INLINE_VECTORS, new InlineVectors.State(vector)));
                    pqVectorsList.add(encoded);
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

    public SegmentMetadata.ComponentMetadataMap writeData() throws IOException
    {
        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                        nextOrdinal, builder.getGraph().size());
        assert pqVectors.count() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                              pqVectors.count(), builder.getGraph().size());
        assert postingsMap.keySet().size() == builder.getGraph().size() : String.format("postings map entry count %d != vector count %d",
                                                                                        postingsMap.keySet().size(), builder.getGraph().size());
        logger.debug("Writing graph with {} rows and {} distinct vectors",
                     postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), builder.getGraph().size());

        try (var postingsOutput = IndexFileUtils.instance.openOutput(descriptor.fileFor(IndexComponent.POSTING_LISTS, context), true);
             var pqOutput = IndexFileUtils.instance.openOutput(descriptor.fileFor(IndexComponent.PQ, context), true))
        {
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(pqOutput);

            // write PQ
            long pqOffset = pqOutput.getFilePointer();
            pqVectors.write(pqOutput.asSequentialWriter());
            long pqLength = pqOutput.getFilePointer() - pqOffset;

            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<Integer>(postingsOneToOne, i -> i)
                                            .writePostings(postingsOutput.asSequentialWriter(), inlineVectors, postingsMap, Set.of());
            long postingsLength = postingsPosition - postingsOffset;

            // complete (internal clean up) and write the graph
            builder.cleanup();

            var start = System.nanoTime();
            writer.write(new EnumMap<>(FeatureId.class));
            SAICodecUtils.writeFooter(writer.getOutput(), writer.checksum());
            logger.info("Writing graph took {}ms", (System.nanoTime() - start) / 1_000_000);
            long termsLength = writer.getOutput().position() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength, pqOffset, pqLength, postingsOffset, postingsLength);
        }
    }

    public long ramBytesUsed()
    {
        return pqVectors.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    private long exactRamBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }
}
