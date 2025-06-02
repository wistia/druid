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

package org.apache.druid.server.lookup;

import org.apache.druid.server.lookup.cache.loading.LoadingCache;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LoadingLookupWithNullCacheTest extends InitializedNullHandlingTest
{
  private DataFetcher dataFetcher;
  private LoadingCache<String, String> lookupCache;
  private LoadingCache<String, List<String>> reverseLookupCache;
  private LoadingLookupWithNullCache lookup;

  @Before
  public void setUp()
  {
    dataFetcher = EasyMock.createMock(DataFetcher.class);
    lookupCache = EasyMock.createMock(LoadingCache.class);
    reverseLookupCache = EasyMock.createStrictMock(LoadingCache.class);
    lookup = new LoadingLookupWithNullCache(dataFetcher, lookupCache, reverseLookupCache);
  }

  @Test
  public void testGetExistingKey()
  {
    EasyMock.expect(lookupCache.getIfPresent("key1")).andReturn("value1").once();
    EasyMock.replay(lookupCache);
    Assert.assertEquals("value1", lookup.apply("key1"));
    EasyMock.verify(lookupCache);
  }

  @Test
  public void testGetNonExistentKey()
  {
    EasyMock.expect(lookupCache.getIfPresent("nonexistent")).andReturn(null).once();
    EasyMock.expect(dataFetcher.fetch("nonexistent")).andReturn(null).once();
    EasyMock.replay(lookupCache, dataFetcher);
    Assert.assertNull(lookup.apply("nonexistent"));
    Assert.assertNull(lookup.apply("nonexistent"));
    EasyMock.verify(lookupCache, dataFetcher);
  }
}
