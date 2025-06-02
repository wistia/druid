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

package org.apache.druid.server.lookup.ext;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.lookup.DataFetcher;
import org.apache.druid.server.lookup.cache.loading.LoadingCache;
import org.apache.druid.server.lookup.cache.loading.OffHeapLoadingCache;
import org.apache.druid.server.lookup.cache.loading.OnHeapLoadingCache;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LoadingLookupWithNullCacheFactoryTest
{
  private DataFetcher dataFetcher;
  private LoadingCache<String, String> lookupCache;
  private LoadingCache<String, List<String>> reverseLookupCache;
  private LoadingLookupWithNullCacheFactory factory;

  @Before
  public void setUp()
  {
    dataFetcher = EasyMock.createMock(DataFetcher.class);
    lookupCache = EasyMock.createStrictMock(LoadingCache.class);
    reverseLookupCache = EasyMock.createStrictMock(LoadingCache.class);
    factory = new LoadingLookupWithNullCacheFactory(dataFetcher, lookupCache, reverseLookupCache);
  }

  @Test
  public void testStartStop()
  {
    Assert.assertTrue(factory.start());
    factory.awaitInitialization();
    Assert.assertTrue(factory.isInitialized());
    Assert.assertTrue(factory.close());
  }

  @Test
  public void testReplacesWithNull()
  {
    Assert.assertTrue(factory.replaces(null));
  }

  @Test
  public void testReplacesWithSame()
  {
    Assert.assertFalse(factory.replaces(factory));
  }

  @Test
  public void testReplacesWithDifferent()
  {
    Assert.assertTrue(factory.replaces(new LoadingLookupWithNullCacheFactory(
        EasyMock.createMock(DataFetcher.class),
        lookupCache,
        reverseLookupCache
    )));
    Assert.assertTrue(factory.replaces(new LoadingLookupWithNullCacheFactory(
        dataFetcher,
        EasyMock.createMock(LoadingCache.class),
        reverseLookupCache
    )));
    Assert.assertTrue(factory.replaces(new LoadingLookupWithNullCacheFactory(
        dataFetcher,
        lookupCache,
        EasyMock.createMock(LoadingCache.class)
    )));
  }

  @Test
  public void testGet()
  {
    Assert.assertNotNull(factory.get());
  }

  @Test
  public void testSerDeser() throws IOException
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    LoadingLookupWithNullCacheFactory factory = new LoadingLookupWithNullCacheFactory(
        new MockDataFetcher(),
        new OnHeapLoadingCache<>(
            0,
            100,
            100L,
            0L,
            0L
        ),
        new OffHeapLoadingCache<>(
            100,
            100L,
            0L,
            0L
        )
    );

    mapper.registerSubtypes(MockDataFetcher.class);
    mapper.registerSubtypes(LoadingLookupWithNullCacheFactory.class);
    Assert.assertEquals(
        factory,
        mapper.readerFor(LookupExtractorFactory.class)
              .readValue(mapper.writeValueAsString(factory))
    );
  }

  private static class MockDataFetcher implements DataFetcher
  {
    @Override
    public Iterable fetchAll()
    {
      return Collections.emptyMap().entrySet();
    }

    @Override
    public Object fetch(Object key)
    {
      return null;
    }

    @Override
    public Iterable fetch(Iterable keys)
    {
      return null;
    }

    @Override
    public List reverseFetchKeys(Object value)
    {
      return null;
    }

    @Override
    public int hashCode()
    {
      return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof MockDataFetcher;
    }
  }
}
