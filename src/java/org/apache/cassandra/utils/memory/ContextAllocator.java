package org.apache.cassandra.utils.memory;

import com.google.common.base.*;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.utils.concurrent.OpOrdering;
import org.apache.cassandra.db.ColumnFamilyStore;

import java.nio.ByteBuffer;

/**
 * Wraps calls to a PoolAllocator with the provided writeOp. Also doubles as a Function that clones Cells
 * using itself
 */
public final class ContextAllocator extends AbstractAllocator implements Function<Cell, Cell>
{
    private final OpOrdering.Ordered writeOp;
    private final PoolAllocator allocator;
    private final ColumnFamilyStore cfs;

    public ContextAllocator(OpOrdering.Ordered writeOp, PoolAllocator allocator, ColumnFamilyStore cfs)
    {
        this.writeOp = writeOp;
        this.allocator = allocator;
        this.cfs = cfs;
    }

    @Override
    public ByteBuffer clone(ByteBuffer buffer)
    {
        return allocator.clone(buffer, writeOp);
    }

    public ByteBuffer allocate(int size)
    {
        return allocator.allocate(size, writeOp);
    }

    public Cell apply(Cell column)
    {
        return column.localCopy(cfs, this);
    }
}
