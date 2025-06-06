/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.join.table;

import org.apache.druid.segment.ReferenceCountingCloseableObject;
import org.apache.druid.segment.column.RowSignature;

import java.io.Closeable;
import java.util.Optional;
import java.util.Set;

public class ReferenceCountedIndexedTableProvider extends ReferenceCountingCloseableObject<IndexedTable>
    implements IndexedTable
{
  public ReferenceCountedIndexedTableProvider(IndexedTable indexedTable)
  {
    super(indexedTable);
  }

  @Override
  public String version()
  {
    return baseObject.version();
  }

  @Override
  public Set<String> keyColumns()
  {
    return baseObject.keyColumns();
  }

  @Override
  public RowSignature rowSignature()
  {
    return baseObject.rowSignature();
  }

  @Override
  public int numRows()
  {
    return baseObject.numRows();
  }

  @Override
  public Index columnIndex(int column)
  {
    return baseObject.columnIndex(column);
  }

  @Override
  public Reader columnReader(int column)
  {
    return baseObject.columnReader(column);
  }

  @Override
  public Optional<Closeable> acquireReference()
  {
    return incrementReferenceAndDecrementOnceCloseable();
  }

  @Override
  public byte[] computeCacheKey()
  {
    return baseObject.computeCacheKey();
  }

  @Override
  public boolean isCacheable()
  {
    return baseObject.isCacheable();
  }
}
