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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.lookup.DataFetcher;
import org.apache.druid.server.lookup.LoadingLookup;
import org.apache.druid.server.lookup.cache.loading.LoadingCache;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * LoadingLookupWithNullCache extends LoadingLookup to add caching of null results.
 * This means that within a single query, repeated lookups for keys that return null
 * will be cached to avoid redundant data fetches and cache lookups.
 */
public class LoadingLookupWithNullCache extends LoadingLookup
{
  private static final Logger LOGGER = new Logger(LoadingLookupWithNullCache.class);
  private final Set<String> nullCache;

  public LoadingLookupWithNullCache(
      DataFetcher dataFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache
  )
  {
    super(dataFetcher, loadingCache, reverseLoadingCache);
    this.nullCache = new HashSet<>();
  }

  @Override
  public String apply(@Nullable final String key)
  {
    if (key == null) {
      return null;
    }

    if (nullCache.contains(key)) {
      LOGGER.debug("null cache hit for key [%s]", key);
      return null;
    }

    // If not in null cache, use parent's implementation
    String value = super.apply(key);

    // Only cache null results
    if (value == null) {
      LOGGER.debug("null cached for key [%s]", key);
      nullCache.add(key);
    } else {
      LOGGER.debug("value cached for key [%s]", key);
    }

    return value;
  }
}
