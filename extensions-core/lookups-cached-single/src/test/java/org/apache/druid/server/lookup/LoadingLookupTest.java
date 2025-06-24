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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.server.lookup.cache.loading.LoadingCache;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class LoadingLookupTest extends InitializedNullHandlingTest
{
  DataFetcher dataFetcher = EasyMock.createMock(DataFetcher.class);
  LoadingCache lookupCache = EasyMock.createMock(LoadingCache.class);
  LoadingCache reverseLookupCache = EasyMock.createStrictMock(LoadingCache.class);
  LoadingLookup loadingLookup = new LoadingLookup(dataFetcher, lookupCache, reverseLookupCache);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testApplyEmptyOrNull()
  {
    EasyMock.expect(lookupCache.getIfPresent(EasyMock.eq("")))
            .andReturn("empty").atLeastOnce();
    EasyMock.replay(lookupCache);
    Assert.assertEquals("empty", loadingLookup.apply(""));
    Assert.assertNull(loadingLookup.apply(null));
    EasyMock.verify(lookupCache);
  }

  @Test
  public void testUnapplyNull()
  {
    Assert.assertEquals(Collections.emptyList(), loadingLookup.unapply(null));
  }

  @Test
  public void testApply()
  {
    EasyMock.expect(lookupCache.getIfPresent(EasyMock.eq("key"))).andReturn("value").once();
    EasyMock.replay(lookupCache);
    Assert.assertEquals(ImmutableMap.of("key", "value"), loadingLookup.applyAll(ImmutableSet.of("key")));
    EasyMock.verify(lookupCache);
  }

  @Test
  public void testApplyWithNullValue()
  {
    EasyMock.expect(lookupCache.getIfPresent(EasyMock.eq("key"))).andReturn(null).once();
    EasyMock.expect(dataFetcher.fetch("key")).andReturn(null).once();
    EasyMock.replay(lookupCache, dataFetcher);
    Assert.assertNull(loadingLookup.apply("key"));
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyTriggersCacheMissAndSubsequentCacheHit()
  {
    Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    EasyMock.expect(lookupCache.getIfPresent(EasyMock.eq("key"))).andReturn(null).once();
    EasyMock.expect(dataFetcher.fetch("key")).andReturn("value").once();
    lookupCache.putAll(map);
    EasyMock.expectLastCall().andVoid();
    EasyMock.expect(lookupCache.getIfPresent("key")).andReturn("value").once();
    EasyMock.replay(lookupCache, dataFetcher);
    Assert.assertEquals(loadingLookup.apply("key"), "value");
    Assert.assertEquals(loadingLookup.apply("key"), "value");
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testUnapplyAll() throws ExecutionException
  {
    EasyMock.expect(reverseLookupCache.get(EasyMock.eq("value"), EasyMock.anyObject(Callable.class)))
            .andReturn(Collections.singletonList("key"))
            .once();
    EasyMock.replay(reverseLookupCache);
    Assert.assertEquals(
        Collections.singletonList("key"),
        Lists.newArrayList(loadingLookup.unapplyAll(ImmutableSet.of("value")))
    );
    EasyMock.verify(reverseLookupCache);
  }

  @Test
  public void testClose()
  {
    lookupCache.close();
    reverseLookupCache.close();
    EasyMock.replay(lookupCache, reverseLookupCache);
    loadingLookup.close();
    EasyMock.verify(lookupCache, reverseLookupCache);
  }

  @Test
  public void testUnApplyWithExecutionError() throws ExecutionException
  {
    EasyMock.expect(reverseLookupCache.get(EasyMock.eq("value"), EasyMock.anyObject(Callable.class)))
            .andThrow(new ExecutionException(null))
            .once();
    EasyMock.replay(reverseLookupCache);
    Assert.assertEquals(Collections.emptyList(), loadingLookup.unapply("value"));
    EasyMock.verify(reverseLookupCache);
  }

  @Test
  public void testGetCacheKey()
  {
    Assert.assertFalse(Arrays.equals(loadingLookup.getCacheKey(), loadingLookup.getCacheKey()));
  }

  @Test
  public void testSupportsAsMap()
  {
    Assert.assertTrue(loadingLookup.supportsAsMap());
  }

  @Test
  public void testAsMap()
  {
    final Map<String, String> fetchedData = new HashMap<>();
    fetchedData.put("dummy", "test");
    fetchedData.put("key", null);
    fetchedData.put(null, "value");
    EasyMock.expect(dataFetcher.fetchAll()).andReturn(fetchedData.entrySet());
    EasyMock.replay(dataFetcher);
    Assert.assertEquals(loadingLookup.asMap(), fetchedData);
    EasyMock.verify(dataFetcher);
  }

  @Test
  public void testApplyAllWithNullInput()
  {
    EasyMock.replay(lookupCache, dataFetcher);
    Assert.assertEquals(Collections.emptyList(), loadingLookup.applyAll(null));
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithEmptyCollection()
  {
    EasyMock.replay(lookupCache, dataFetcher);
    Assert.assertEquals(Collections.emptyList(), loadingLookup.applyAll(Collections.emptyList()));
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithEmptyIterable()
  {
    EasyMock.replay(lookupCache, dataFetcher);
    Assert.assertEquals(Collections.emptyList(), loadingLookup.applyAll(Collections.emptySet()));
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithAllCacheHits()
  {
    Set<String> keys = ImmutableSet.of("key1", "key2", "key3");
    Map<String, String> cachedValues = ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3");

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList("value1", "value2", "value3"), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithAllCacheMisses()
  {
    Set<String> keys = ImmutableSet.of("key1", "key2");
    Map<String, String> cachedValues = Collections.emptyMap();
    Map<String, String> fetchedValues = ImmutableMap.of("key1", "value1", "key2", "value2");

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(keys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList("value1", "value2"), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithMixedCacheHitsAndMisses()
  {
    Set<String> keys = ImmutableSet.of("key1", "key2", "key3", "key4");
    Map<String, String> cachedValues = ImmutableMap.of("key1", "value1", "key3", "value3");
    Set<String> missingKeys = ImmutableSet.of("key2", "key4");
    Map<String, String> fetchedValues = ImmutableMap.of("key2", "value2", "key4", "value4");

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(missingKeys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList("value1", "value2", "value3", "value4"), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithSomeNullValues()
  {
    Set<String> keys = ImmutableSet.of("key1", "key2", "key3");
    Map<String, String> cachedValues = ImmutableMap.of("key1", "value1");
    Set<String> missingKeys = ImmutableSet.of("key2", "key3");
    Map<String, String> fetchedValues = ImmutableMap.of("key2", null, "key3", "value3");

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(missingKeys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList("value1", null, "value3"), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithEmptyFetchedResults()
  {
    Set<String> keys = ImmutableSet.of("key1", "key2");
    Map<String, String> cachedValues = Collections.emptyMap();
    Map<String, String> fetchedValues = Collections.emptyMap();

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(keys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList(null, null), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithDuplicateKeys()
  {
    List<String> keys = Arrays.asList("key1", "key2", "key1", "key3");
    Set<String> uniqueKeys = ImmutableSet.of("key1", "key2", "key3");
    Map<String, String> cachedValues = ImmutableMap.of("key1", "value1");
    Set<String> missingKeys = ImmutableSet.of("key2", "key3");
    Map<String, String> fetchedValues = ImmutableMap.of("key2", "value2", "key3", "value3");

    EasyMock.expect(lookupCache.getAllPresent(uniqueKeys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(missingKeys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList("value1", "value2", "value1", "value3"), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithSingleKey()
  {
    Set<String> keys = ImmutableSet.of("key1");
    Map<String, String> cachedValues = Collections.emptyMap();
    Map<String, String> fetchedValues = ImmutableMap.of("key1", "value1");

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(keys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    Assert.assertEquals(Arrays.asList("value1"), result);
    EasyMock.verify(lookupCache, dataFetcher);
  }

  @Test
  public void testApplyAllWithLargeKeySet()
  {
    Set<String> keys = ImmutableSet.of("key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10");
    Map<String, String> cachedValues = ImmutableMap.of("key1", "value1", "key5", "value5", "key10", "value10");
    Set<String> missingKeys = ImmutableSet.of("key2", "key3", "key4", "key6", "key7", "key8", "key9");
    Map<String, String> fetchedValues = ImmutableMap.of(
        "key2", "value2", "key3", "value3", "key4", "value4",
        "key6", "value6", "key7", "value7", "key8", "value8", "key9", "value9"
    );

    EasyMock.expect(lookupCache.getAllPresent(keys)).andReturn(cachedValues).once();
    EasyMock.expect(dataFetcher.fetch(missingKeys)).andReturn(fetchedValues.entrySet()).once();
    lookupCache.putAll(fetchedValues);
    EasyMock.expectLastCall().andVoid();
    EasyMock.replay(lookupCache, dataFetcher);

    List<String> result = loadingLookup.applyAll(keys);
    List<String> expected = Arrays.asList("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10");
    Assert.assertEquals(expected, result);
    EasyMock.verify(lookupCache, dataFetcher);
  }
}
