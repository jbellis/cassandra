package org.apache.cassandra.io.util;

import java.io.File;

import org.apache.cassandra.io.sstable.Descriptor;

public class ChecksummedSequentialWriter extends SequentialWriter
{
    private final SequentialWriter crcWriter;
    private final DataIntegrityMetadata.ChecksumWriter crcMetdata;

    public ChecksummedSequentialWriter(File file, int bufferSize, boolean skipIOCache, File crcPath)
    {
        super(file, bufferSize, skipIOCache);
        crcWriter = new SequentialWriter(crcPath, 8 * 1024, true);
        crcMetdata = new DataIntegrityMetadata.ChecksumWriter(crcWriter.stream);
        crcMetdata.writeChunkSize(buffer.length);
    }

    protected void flushData()
    {
        super.flushData();
        crcMetdata.append(buffer, 0, validBufferBytes);
    }

    public void writeFullChecksum(Descriptor descriptor)
    {
        crcMetdata.writeFullChecksum(descriptor);
    }

    public void close()
    {
        super.close();
        crcWriter.close();
    }
}
