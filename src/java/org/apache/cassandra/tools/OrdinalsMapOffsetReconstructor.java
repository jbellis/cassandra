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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrdinalsMapOffsetReconstructor
{
    private final RandomAccessFile reader;
    public final List<Segment> segments = new ArrayList<>();

    public static class Segment {
        public final long offset;
        public final int vectorCount;
        public final int lastRowId;

        public Segment(long offset, int vectorCount, int lastRowId) {
            this.offset = offset;
            this.vectorCount = vectorCount;
            this.lastRowId = lastRowId;
        }
    }

    public OrdinalsMapOffsetReconstructor(File file) throws IOException {
        this.reader = new RandomAccessFile(file, "r");
        reconstructSegmentOffsets();
        reader.close();
    }

    public long readRowCount(int nVectors) throws IOException {
        long totalNumberOfRows = 0;

        // Iterate through the offsets to read the size of each postings list
        long postingsOffset = reader.readLong(); // Read the offset for the postings list
        reader.seek(postingsOffset); // Seek to the start of the postings list
        for (int i = 0; i < nVectors; i++) {
            int postingsSize = reader.readInt(); // Read the size of the postings list
            totalNumberOfRows += postingsSize; // Add the size to the total
            reader.seek(reader.getFilePointer() + postingsSize * 4L); // Skip the postings list
        }

        return totalNumberOfRows;
    }

    public void reconstructSegmentOffsets() throws IOException {
        long offset = 0;
        while (offset < reader.length()) {
            long segmentOffset = offset;
            // Read the deleted count and calculate the offset after deleted ordinals
            reader.seek(offset);
            int deletedCount = reader.readInt();
            offset += 4 + 4L * deletedCount;

            // Read and calculate the offset after NodeOrdinalToRowIdMapping
            reader.seek(offset);
            int nVectors = reader.readInt();
            long rowCount = readRowCount(nVectors);
            long rowNodeOffset = reader.getFilePointer();

            // the row-to-node mapping is 8 bytes for each pair
            reader.seek(reader.getFilePointer() + 8 * (rowCount - 1));
            // read the last row id
            int lastRow = reader.readInt();
            reader.readInt(); // don't care about the ordinal value

            // after that mapping the last entry is the rowNodeOffset so let's validate that they match
            long checkedRowOffset = reader.readLong();
            assert rowNodeOffset == checkedRowOffset : "RowNodeOffset " + rowNodeOffset + " does not match the last entry in the row-to-node mapping " + reader.readLong();

            offset = reader.getFilePointer();
            segments.add(new Segment(segmentOffset, nVectors, lastRow));
        }

        segments.add(new Segment(reader.length(), -1, -1)); // simplifies scanning logic
    }
}
