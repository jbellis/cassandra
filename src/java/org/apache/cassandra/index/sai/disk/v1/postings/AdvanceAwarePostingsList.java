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

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.PostingList;

public interface AdvanceAwarePostingsList extends PostingList
{
    int lastAdvancedCount();

    static AdvanceAwarePostingsList wrap(OrdinalPostingList delegate)
    {
        return new AdvanceAwarePostingsList()
        {
            private long beginOrdinal = 0;

            @Override
            public int lastAdvancedCount()
            {
                if (beginOrdinal < 0)
                    throw new IllegalStateException("lastAdvancedCount may only be called immediately after advance");
                return (int) (delegate.getOrdinal() - beginOrdinal);
            }

            @Override
            public long nextPosting() throws IOException
            {
                beginOrdinal = -1;
                return delegate.nextPosting();
            }

            @Override
            public long size()
            {
                return delegate.size();
            }

            @Override
            public long advance(long targetRowID) throws IOException
            {
                beginOrdinal = delegate.getOrdinal();
                return delegate.advance(targetRowID);
            }
        };
    }
}
