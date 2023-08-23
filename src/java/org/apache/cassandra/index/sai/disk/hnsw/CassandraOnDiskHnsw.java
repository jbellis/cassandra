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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.util.concurrent.MoreExecutors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.ReorderingPostingList;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.HnswSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;

public class CassandraOnDiskHnsw implements AutoCloseable
{
    private static final Logger logger = Logger.getLogger(CassandraOnDiskHnsw.class.getName());

    private final Function<QueryContext, VectorsWithCache> vectorsSupplier;
    private final OnDiskOrdinalsMap ordinalsMap;
    private final OnDiskHnswGraph hnsw;
    private final VectorSimilarityFunction similarityFunction;

    // VSTODO wire up cache to metrics
    private final Cache<Integer, float[]> vectorCache = Caffeine.newBuilder()
                                                                .maximumWeight(CassandraRelevantProperties.SAI_HNSW_VECTOR_CACHE_BYTES.getInt())
                                                                .weigher((Integer k, float[] v) -> 36 + v.length * 4)
                                                                .executor(MoreExecutors.directExecutor())
                                                                .build();

    private static final int OFFSET_CACHE_MIN_BYTES = 100_000;

    private static Map<String, AtomicInteger> offsetsHack = new ConcurrentHashMap<>();
    public CassandraOnDiskHnsw(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles, IndexContext context) throws IOException
    {
        similarityFunction = context.getIndexWriterConfig().getSimilarityFunction();

        // FIXME this reads the offset from the TOC instead of the metadata
//        long pqSegmentOffset = componentMetadatas.get(IndexComponent.PQ).offset;
        long pqSegmentOffset;
        try (var in = indexFiles.pq().createReader()) {

            var ai = offsetsHack.computeIfAbsent(indexFiles.pq().createReader().getFile().absolutePath(),
                                                 (k) -> new AtomicInteger());
            in.seek(in.length() - 4);
            int count = in.readInt() + 1; // we don't write offset 0 to TOC
            int n = ai.getAndIncrement();
            if (n == 0) {
                pqSegmentOffset = 0;
            } else
            {
                in.seek(in.length() - 4 - (8L * (count - n)));
                pqSegmentOffset = in.readInt();
            }
        }
        var compressedVectors = CompressedVectors.load(indexFiles.pq(), pqSegmentOffset);

        long vectorsSegmentOffset = componentMetadatas.get(IndexComponent.VECTOR).offset;
        vectorsSupplier = (qc) -> {
            OnDiskVectors odv = new OnDiskVectors(indexFiles.vectors(), vectorsSegmentOffset);
            return new VectorsWithCache(odv, compressedVectors);
        };

        SegmentMetadata.ComponentMetadata postingListsMetadata = componentMetadatas.get(IndexComponent.POSTING_LISTS);
        ordinalsMap = new OnDiskOrdinalsMap(indexFiles.postingLists(), postingListsMetadata.offset, postingListsMetadata.length);

        SegmentMetadata.ComponentMetadata termsMetadata = componentMetadatas.get(IndexComponent.TERMS_DATA);
        hnsw = new OnDiskHnswGraph(indexFiles.termsData(), termsMetadata.offset, termsMetadata.length, OFFSET_CACHE_MIN_BYTES);
    }

    public long ramBytesUsed()
    {
        return hnsw.getCacheSizeInBytes() + vectorCache.estimatedSize();
    }

    public int size()
    {
        return hnsw.size();
    }

    /**
     * @return Row IDs associated with the topK vectors near the query
     */
    // VSTODO make this return something with a size
    public ReorderingPostingList search(float[] queryVector, int topK, Bits acceptBits, int vistLimit, QueryContext context)
    {
        CassandraOnHeapHnsw.validateIndexable(queryVector, similarityFunction);

        try (var vectors = vectorsSupplier.apply(context); var view = hnsw.getView(context))
        {
            var queue = new HnswSearcher.Builder<>(view,
                                                   vectors.originalVectors,
                                                   (i) -> vectors.approximateSimilarity(i, queryVector, similarityFunction))
                        .build()
                        .search(topK * 2, ordinalsMap.ignoringDeleted(acceptBits), vistLimit);
            return annRowIdsToPostings(queryVector, queue, vectors, topK);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private class RowIdIterator implements PrimitiveIterator.OfInt, AutoCloseable
    {
        private final OfInt ordinals;
        private final OnDiskOrdinalsMap.RowIdsView rowIdsView = ordinalsMap.getRowIdsView();

        private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();

        public RowIdIterator(OfInt ordinals)
        {
            this.ordinals = ordinals;
        }

        @Override
        public boolean hasNext() {
            while (!segmentRowIdIterator.hasNext() && ordinals.hasNext()) {
                try
                {
                    var ordinal = ordinals.next();
                    segmentRowIdIterator = Arrays.stream(rowIdsView.getSegmentRowIdsMatching(ordinal)).iterator();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return segmentRowIdIterator.hasNext();
        }

        @Override
        public int nextInt() {
            if (!hasNext())
                throw new NoSuchElementException();
            return segmentRowIdIterator.nextInt();
        }

        @Override
        public void close()
        {
            rowIdsView.close();
        }
    }

    private ReorderingPostingList annRowIdsToPostings(float[] queryVector, NeighborQueue queue, VectorsWithCache vectors, int topK) throws IOException
    {
        // order the top K results by their true similarity
        // VSTODO is the boxing here material?
        Pair<Integer, Float>[] nodesWithScore = new Pair[queue.size()];
        for (int i = 0; i < nodesWithScore.length; i++)
        {
            var n = queue.pop();
            var score = similarityFunction.compare(queryVector, vectors.originalVectors.vectorValue(i));
            nodesWithScore[i] = Pair.create(n, score);
        }
        // sort both nodes and scores by their respective scores
        Arrays.sort(nodesWithScore, Comparator.comparingDouble((Pair<Integer, Float> p) -> p.right).reversed());
        var nodes = Arrays.stream(nodesWithScore).limit(topK).mapToInt(p -> p.left).iterator();

        try (var iterator = new RowIdIterator(nodes))
        {
            return new ReorderingPostingList(iterator, nodesWithScore.length);
        }
    }

    public void close()
    {
        ordinalsMap.close();
        hnsw.close();
    }

    public OnDiskOrdinalsMap.OrdinalsView getOrdinalsView() throws IOException
    {
        return ordinalsMap.getOrdinalsView();
    }

    @NotThreadSafe
    class VectorsWithCache implements AutoCloseable
    {
        private final OnDiskVectors originalVectors;
        private final CompressedVectors compressedVectors;
        private final List<float[]> inMemoryOriginals;

        public VectorsWithCache(OnDiskVectors originalVectors, CompressedVectors compressedVectors)
        {
            this.originalVectors = originalVectors;
            this.compressedVectors = compressedVectors;
            if (compressedVectors == null)
            {
                // FIXME this will run for every query
                inMemoryOriginals = new ArrayList<>(originalVectors.size());
                for (int i = 0; i < originalVectors.size(); i++)
                {
                    inMemoryOriginals.add(originalVectors.vectorValue(i));
                }
            }
            else
            {
                inMemoryOriginals = null;
            }
        }

        public int size()
        {
            return originalVectors.size();
        }

        public int dimension()
        {
            return originalVectors.dimension();
        }

        public float[] originalVector(int i)
        {
            return vectorCache.get(i, originalVectors::vectorValue);
        }

        public float approximateSimilarity(int ordinal, float[] other, VectorSimilarityFunction similarityFunction)
        {
            if (compressedVectors == null) {
                return similarityFunction.compare(inMemoryOriginals.get(ordinal), other);
            }
            return compressedVectors.decodedSimilarity(ordinal, other, similarityFunction);
        }

        public void close()
        {
            originalVectors.close();
        }
    }
}
