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
import java.util.List;

public class OrdinalsMapOffsetReconstructor
{
    private final RandomAccessFile reader;
    private List<Long> segmentOffsets;
    private int vectorCount;
    private int lastRow;

    public OrdinalsMapOffsetReconstructor(File file) throws IOException {
        this.reader = new RandomAccessFile(file, "r");
        reconstructSegmentOffsets();
        reader.close();
    }

    public long readRowCount() throws IOException {
        long totalNumberOfRows = 0;

        // Iterate through the offsets to read the size of each postings list
        long postingsOffset = reader.readLong(); // Read the offset for the postings list
        reader.seek(postingsOffset); // Seek to the start of the postings list
        for (int i = 0; i < vectorCount; i++) {
            int postingsSize = reader.readInt(); // Read the size of the postings list
            totalNumberOfRows += postingsSize; // Add the size to the total
            reader.seek(reader.getFilePointer() + postingsSize * 4L); // Skip the postings list
        }

        return totalNumberOfRows;
    }

    public void reconstructSegmentOffsets() throws IOException {
        this.segmentOffsets = new ArrayList<>();
        long offset = 0;

        while (offset < reader.length()) {
            segmentOffsets.add(offset);
            // Read the deleted count and calculate the offset after deleted ordinals
            reader.seek(offset);
            int deletedCount = reader.readInt();
            offset += 4 + 4L * deletedCount;

            // Read and calculate the offset after NodeOrdinalToRowIdMapping
            reader.seek(offset);
            this.vectorCount = reader.readInt();
            long rowCount = readRowCount();
            long rowNodeOffset = reader.getFilePointer();

            // the row-to-node mapping is 8 bytes for each pair
            reader.seek(reader.getFilePointer() + 8 * (rowCount - 1));
            // read the last row id
            this.lastRow = reader.readInt();
            reader.readInt(); // don't care about the ordinal value

            // after that mapping the last entry is the rowNodeOffset so let's validate that they match
            long checkedRowOffset = reader.readLong();
            assert rowNodeOffset == checkedRowOffset : "RowNodeOffset " + rowNodeOffset + " does not match the last entry in the row-to-node mapping " + reader.readLong();

            offset = reader.getFilePointer();
        }

        segmentOffsets.add(reader.getFilePointer()); // simplifies scanning logic
    }

    public List<Long> getSegmentOffsets()
    {
        return segmentOffsets;
    }

    public int getVectorCount()
    {
        return vectorCount;
    }

    public int getLastRowId()
    {
        return lastRow;
    }
}
