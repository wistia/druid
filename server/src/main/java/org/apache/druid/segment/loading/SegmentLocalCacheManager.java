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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public class SegmentLocalCacheManager implements SegmentCacheManager
{
  @VisibleForTesting
  static final String DOWNLOAD_START_MARKER_FILE_NAME = "downloadStartMarker";

  private static final EmittingLogger log = new EmittingLogger(SegmentLocalCacheManager.class);

  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  // This directoryWriteRemoveLock is used when creating or removing a directory
  private final Object directoryWriteRemoveLock = new Object();

  /**
   * A map between segment and referenceCountingLocks.
   *
   * These locks should be acquired whenever getting or deleting files for a segment.
   * If different threads try to get or delete files simultaneously, one of them creates a lock first using
   * {@link #createOrGetLock}. And then, all threads compete with each other to get the lock.
   * Finally, the lock should be released using {@link #unlock}.
   *
   * An example usage is:
   *
   * final ReferenceCountingLock lock = createOrGetLock(segment);
   * synchronized (lock) {
   *   try {
   *     doSomething();
   *   }
   *   finally {
   *     unlock(lock);
   *   }
   * }
   */
  private final ConcurrentHashMap<DataSegment, ReferenceCountingLock> segmentLocks = new ConcurrentHashMap<>();

  private final StorageLocationSelectorStrategy strategy;

  private final IndexIO indexIO;

  private ExecutorService loadOnBootstrapExec = null;
  private ExecutorService loadOnDownloadExec = null;

  @Inject
  public SegmentLocalCacheManager(
      List<StorageLocation> locations,
      SegmentLoaderConfig config,
      @Nonnull StorageLocationSelectorStrategy strategy,
      IndexIO indexIO,
      @Json ObjectMapper mapper
  )
  {
    this.config = config;
    this.jsonMapper = mapper;
    this.locations = locations;
    this.strategy = strategy;
    this.indexIO = indexIO;

    log.info("Using storage location strategy[%s].", this.strategy.getClass().getSimpleName());
    log.info(
        "Number of threads to load segments into page cache - on bootstrap: [%d], on download: [%d].",
        config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap(),
        config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload()
    );

    if (config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap() > 0) {
      loadOnBootstrapExec = Execs.multiThreaded(
          config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap(),
          "Load-SegmentsIntoPageCacheOnBootstrap-%s"
      );
    }

    if (config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload() > 0) {
      loadOnDownloadExec = Executors.newFixedThreadPool(
          config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload(),
          Execs.makeThreadFactory("LoadSegmentsIntoPageCacheOnDownload-%s")
      );
    }
  }

  @Override
  public boolean canHandleSegments()
  {
    final boolean isLocationsValid = !(locations == null || locations.isEmpty());
    final boolean isLocationsConfigValid = !(config.getLocations() == null || config.getLocations().isEmpty());
    return isLocationsValid || isLocationsConfigValid;
  }

  @Override
  public List<DataSegment> getCachedSegments() throws IOException
  {
    if (!canHandleSegments()) {
      throw DruidException.defensive(
          "canHandleSegments() is false. getCachedSegments() must be invoked only when canHandleSegments() returns true."
      );
    }
    final File infoDir = getEffectiveInfoDir();
    FileUtils.mkdirp(infoDir);

    final List<DataSegment> cachedSegments = new ArrayList<>();
    final File[] segmentsToLoad = infoDir.listFiles();

    int ignored = 0;

    for (int i = 0; i < segmentsToLoad.length; i++) {
      final File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i + 1, segmentsToLoad.length, file);
      try {
        final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
        if (!segment.getId().toString().equals(file.getName())) {
          log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getId());
          ignored++;
        } else if (isSegmentCached(segment)) {
          cachedSegments.add(segment);
        } else {
          final SegmentId segmentId = segment.getId();
          log.warn("Unable to find cache file for segment[%s]. Deleting lookup entry.", segmentId);
          removeInfoFile(segment);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to load segment from segment cache file.")
           .addData("file", file)
           .emit();
      }
    }

    if (ignored > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignored)
         .emit();
    }

    return cachedSegments;
  }

  @Override
  public void storeInfoFile(DataSegment segment) throws IOException
  {
    final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), segment.getId().toString());
    if (!segmentInfoCacheFile.exists()) {
      jsonMapper.writeValue(segmentInfoCacheFile, segment);
    }
  }

  @Override
  public void removeInfoFile(DataSegment segment)
  {
    final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), segment.getId().toString());
    if (!segmentInfoCacheFile.delete()) {
      log.warn("Unable to delete cache file[%s] for segment[%s].", segmentInfoCacheFile, segment.getId());
    }
  }

  @Override
  public ReferenceCountedSegmentProvider getSegment(final DataSegment dataSegment) throws SegmentLoadingException
  {
    final File segmentFiles = getSegmentFiles(dataSegment);
    final SegmentizerFactory factory = getSegmentFactory(segmentFiles);

    final Segment segment = factory.factorize(dataSegment, segmentFiles, false, SegmentLazyLoadFailCallback.NOOP);
    return ReferenceCountedSegmentProvider.wrapSegment(segment, dataSegment.getShardSpec());
  }

  @Override
  public ReferenceCountedSegmentProvider getBootstrapSegment(
      final DataSegment dataSegment,
      final SegmentLazyLoadFailCallback loadFailed
  ) throws SegmentLoadingException
  {
    final File segmentFiles = getSegmentFiles(dataSegment);
    final SegmentizerFactory factory = getSegmentFactory(segmentFiles);

    final Segment segment = factory.factorize(dataSegment, segmentFiles, config.isLazyLoadOnStart(), loadFailed);
    return ReferenceCountedSegmentProvider.wrapSegment(segment, dataSegment.getShardSpec());
  }

  private SegmentizerFactory getSegmentFactory(final File segmentFiles) throws SegmentLoadingException
  {
    final File factoryJson = new File(segmentFiles, "factory.json");
    final SegmentizerFactory factory;

    if (factoryJson.exists()) {
      try {
        factory = jsonMapper.readValue(factoryJson, SegmentizerFactory.class);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to get segment facotry for %s", e.getMessage());
      }
    } else {
      factory = new MMappedQueryableSegmentizerFactory(indexIO);
    }
    return factory;
  }

  /**
   * Returns the effective segment info directory based on the configuration settings.
   * The directory is selected based on the following configurations injected into this class:
   * <ul>
   *   <li>{@link SegmentLoaderConfig#getInfoDir()} - If {@code infoDir} is set, it is used as the info directory.</li>
   *   <li>{@link SegmentLoaderConfig#getLocations()} - If the info directory is not set, the first location from this list is used.</li>
   *   <li>List of {@link StorageLocation}s injected - If both the info directory and locations list are not set, the
   *   first storage location is used.</li>
   * </ul>
   *
   * @throws DruidException if none of the configurations are set, and the info directory cannot be determined.
   */
  private File getEffectiveInfoDir()
  {
    final File infoDir;
    if (config.getInfoDir() != null) {
      infoDir = config.getInfoDir();
    } else if (!config.getLocations().isEmpty()) {
      infoDir = new File(config.getLocations().get(0).getPath(), "info_dir");
    } else if (!locations.isEmpty()) {
      infoDir = new File(locations.get(0).getPath(), "info_dir");
    } else {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
          .ofCategory(DruidException.Category.NOT_FOUND)
          .build("Could not determine infoDir. Make sure 'druid.segmentCache.infoDir' "
                 + "or 'druid.segmentCache.locations' is set correctly.");
    }
    return infoDir;
  }

  private static String getSegmentDir(DataSegment segment)
  {
    return DataSegmentPusher.getDefaultStorageDir(segment, false);
  }

  /**
   * Checks whether a segment is already cached. It can return false even if {@link #reserve(DataSegment)}
   * has been successful for a segment but is not downloaded yet.
   */
  boolean isSegmentCached(final DataSegment segment)
  {
    return findStoragePathIfCached(segment) != null;
  }

  /**
   * This method will try to find if the segment is already downloaded on any location. If so, the segment path
   * is returned. Along with that, location state is also updated with the segment location. Refer to
   * {@link StorageLocation#maybeReserve(String, DataSegment)} for more details.
   * If the segment files are damaged in any location, they are removed from the location.
   * @param segment - Segment to check
   * @return - Path corresponding to segment directory if found, null otherwise.
   */
  @Nullable
  private File findStoragePathIfCached(final DataSegment segment)
  {
    for (StorageLocation location : locations) {
      String storageDir = getSegmentDir(segment);
      File localStorageDir = location.segmentDirectoryAsFile(storageDir);
      if (localStorageDir.exists()) {
        if (checkSegmentFilesIntact(localStorageDir)) {
          log.warn(
              "[%s] may be damaged. Delete all the segment files and pull from DeepStorage again.",
              localStorageDir.getAbsolutePath()
          );
          cleanupCacheFiles(location.getPath(), localStorageDir);
          location.removeSegmentDir(localStorageDir, segment);
          break;
        } else {
          // Before returning, we also reserve the space. Refer to the StorageLocation#maybeReserve documentation for details.
          location.maybeReserve(storageDir, segment);
          return localStorageDir;
        }
      }
    }
    return null;
  }

  /**
   * check data intact.
   * @param dir segments cache dir
   * @return true means segment files may be damaged.
   */
  private boolean checkSegmentFilesIntact(File dir)
  {
    return checkSegmentFilesIntactWithStartMarker(dir);
  }

  /**
   * If there is 'downloadStartMarker' existed in localStorageDir, the segments files might be damaged.
   * Because each time, Druid will delete the 'downloadStartMarker' file after pulling and unzip the segments from DeepStorage.
   * downloadStartMarker existed here may mean something error during download segments and the segment files may be damaged.
   */
  private boolean checkSegmentFilesIntactWithStartMarker(File localStorageDir)
  {
    final File downloadStartMarker = new File(localStorageDir.getPath(), DOWNLOAD_START_MARKER_FILE_NAME);
    return downloadStartMarker.exists();
  }

  /**
   * Make sure segments files in loc is intact, otherwise function like loadSegments will failed because of segment files is damaged.
   * @param segment
   * @return
   * @throws SegmentLoadingException
   */
  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        File segmentDir = findStoragePathIfCached(segment);
        if (segmentDir != null) {
          return segmentDir;
        }

        return loadSegmentWithRetry(segment);
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  /**
   * If we have already reserved a location before, probably via {@link #reserve(DataSegment)}, then only that location
   * should be tried. Otherwise, we would fetch locations using {@link StorageLocationSelectorStrategy} and try all
   * of them one by one till there is success.
   * Location may fail because of IO failure, most likely in two cases:<p>
   * 1. druid don't have the write access to this location, most likely the administrator doesn't config it correctly<p>
   * 2. disk failure, druid can't read/write to this disk anymore
   * <p>
   * Locations are fetched using {@link StorageLocationSelectorStrategy}.
   */
  private File loadSegmentWithRetry(DataSegment segment) throws SegmentLoadingException
  {
    String segmentDir = getSegmentDir(segment);

    // Try the already reserved location. If location has been reserved outside, then we do not release the location
    // here and simply delete any downloaded files. That is, we revert anything we do in this function and nothing else.
    for (StorageLocation loc : locations) {
      if (loc.isReserved(segmentDir)) {
        File storageDir = loc.segmentDirectoryAsFile(segmentDir);
        boolean success = loadInLocationWithStartMarkerQuietly(loc, segment, storageDir, false);
        if (!success) {
          throw new SegmentLoadingException(
              "Failed to load segment[%s] in reserved location[%s]", segment.getId(), loc.getPath().getAbsolutePath()
          );
        }
        return storageDir;
      }
    }

    // No location was reserved so we try all the locations
    Iterator<StorageLocation> locationsIterator = strategy.getLocations();
    while (locationsIterator.hasNext()) {

      StorageLocation loc = locationsIterator.next();

      // storageDir is the file path corresponding to segment dir
      File storageDir = loc.reserve(segmentDir, segment);
      if (storageDir != null) {
        boolean success = loadInLocationWithStartMarkerQuietly(loc, segment, storageDir, true);
        if (success) {
          return storageDir;
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment[%s] in all locations.", segment.getId());
  }

  /**
   * A helper method over {@link #loadInLocationWithStartMarker(DataSegment, File)} that catches the {@link SegmentLoadingException}
   * and emits alerts.
   * @param loc - {@link StorageLocation} where segment is to be downloaded in.
   * @param segment - {@link DataSegment} to download
   * @param storageDir - {@link File} pointing to segment directory
   * @param releaseLocation - Whether to release the location in case of failures
   * @return - True if segment was downloaded successfully, false otherwise.
   */
  private boolean loadInLocationWithStartMarkerQuietly(StorageLocation loc, DataSegment segment, File storageDir, boolean releaseLocation)
  {
    try {
      loadInLocationWithStartMarker(segment, storageDir);
      return true;
    }
    catch (SegmentLoadingException e) {
      try {
        log.makeAlert(
            e,
            "Failed to load segment in current location [%s], try next location if any",
            loc.getPath().getAbsolutePath()
        ).addData("location", loc.getPath().getAbsolutePath()).emit();
      }
      finally {
        if (releaseLocation) {
          loc.removeSegmentDir(storageDir, segment);
        }
        cleanupCacheFiles(loc.getPath(), storageDir);
      }
    }
    return false;
  }

  private void loadInLocationWithStartMarker(DataSegment segment, File storageDir) throws SegmentLoadingException
  {
    // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
    // the parent directories of the segment are removed
    final File downloadStartMarker = new File(storageDir, DOWNLOAD_START_MARKER_FILE_NAME);
    synchronized (directoryWriteRemoveLock) {
      try {
        FileUtils.mkdirp(storageDir);

        if (!downloadStartMarker.createNewFile()) {
          throw new SegmentLoadingException("Was not able to create new download marker for [%s]", storageDir);
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
      }
    }
    loadInLocation(segment, storageDir);

    if (!downloadStartMarker.delete()) {
      throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
    }
  }

  private void loadInLocation(DataSegment segment, File storageDir) throws SegmentLoadingException
  {
    // LoadSpec isn't materialized until here so that any system can interpret Segment without having to have all the
    // LoadSpec dependencies.
    final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
    final LoadSpec.LoadSpecResult result = loadSpec.loadSegment(storageDir);
    if (result.getSize() != segment.getSize()) {
      log.warn(
          "Segment [%s] is different than expected size. Expected [%d] found [%d]",
          segment.getId(),
          segment.getSize(),
          result.getSize()
      );
    }
  }

  @Override
  public boolean reserve(final DataSegment segment)
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        // Maybe the segment was already loaded. This check is required to account for restart scenarios.
        if (null != findStoragePathIfCached(segment)) {
          return true;
        }

        String storageDirStr = getSegmentDir(segment);

        // check if we already reserved the segment
        for (StorageLocation location : locations) {
          if (location.isReserved(storageDirStr)) {
            return true;
          }
        }

        // Not found in any location, reserve now
        for (Iterator<StorageLocation> it = strategy.getLocations(); it.hasNext(); ) {
          StorageLocation location = it.next();
          if (null != location.reserve(storageDirStr, segment)) {
            return true;
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }

    return false;
  }

  @Override
  public boolean release(final DataSegment segment)
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        String storageDir = getSegmentDir(segment);

        // Release the first location encountered
        for (StorageLocation location : locations) {
          if (location.isReserved(storageDir)) {
            File localStorageDir = location.segmentDirectoryAsFile(storageDir);
            if (localStorageDir.exists()) {
              throw new ISE(
                  "Asking to release a location '%s' while the segment directory '%s' is present on disk. Any state on disk must be deleted before releasing",
                  location.getPath().getAbsolutePath(),
                  localStorageDir.getAbsolutePath()
              );
            }
            return location.release(storageDir, segment.getSize());
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }

    return false;
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        File loc = findStoragePathIfCached(segment);

        if (loc == null) {
          log.warn("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment.getId());
          return;
        }
        // If storageDir.mkdirs() success, but downloadStartMarker.createNewFile() failed,
        // in this case, findStorageLocationIfLoaded() will think segment is located in the failed storageDir which is actually not.
        // So we should always clean all possible locations here
        for (StorageLocation location : locations) {
          File localStorageDir = new File(location.getPath(), getSegmentDir(segment));
          if (localStorageDir.exists()) {
            // Druid creates folders of the form dataSource/interval/version/partitionNum.
            // We need to clean up all these directories if they are all empty.
            cleanupCacheFiles(location.getPath(), localStorageDir);
            location.removeSegmentDir(localStorageDir, segment);
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  @Override
  public void loadSegmentIntoPageCache(DataSegment segment)
  {
    if (loadOnDownloadExec == null) {
      return;
    }

    loadOnDownloadExec.submit(() -> loadSegmentIntoPageCacheInternal(segment));
  }

  @Override
  public void loadSegmentIntoPageCacheOnBootstrap(DataSegment segment)
  {
    if (loadOnBootstrapExec == null) {
      return;
    }

    loadOnBootstrapExec.submit(() -> loadSegmentIntoPageCacheInternal(segment));
  }

  void loadSegmentIntoPageCacheInternal(DataSegment segment)
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        for (StorageLocation location : locations) {
          File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment, false));
          if (localStorageDir.exists()) {
            File baseFile = location.getPath();
            if (localStorageDir.equals(baseFile)) {
              continue;
            }

            log.info("Loading directory[%s] into page cache.", localStorageDir);

            File[] children = localStorageDir.listFiles();
            if (children != null) {
              for (File child : children) {
                try (InputStream in = Files.newInputStream(child.toPath())) {
                  IOUtils.copy(in, NullOutputStream.NULL_OUTPUT_STREAM);
                  log.info("Loaded [%s] into page cache.", child.getAbsolutePath());
                }
                catch (Exception e) {
                  log.error(e, "Failed to load [%s] into page cache", child.getAbsolutePath());
                }
              }
            }
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  @Override
  public void shutdownBootstrap()
  {
    if (loadOnBootstrapExec == null) {
      return;
    }
    loadOnBootstrapExec.shutdown();
  }

  private void cleanupCacheFiles(File baseFile, File cacheFile)
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    synchronized (directoryWriteRemoveLock) {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error(e, "Unable to remove directory[%s]", cacheFile);
      }

      File parent = cacheFile.getParentFile();
      if (parent != null) {
        File[] children = parent.listFiles();
        if (children == null || children.length == 0) {
          cleanupCacheFiles(baseFile, parent);
        }
      }
    }
  }

  private ReferenceCountingLock createOrGetLock(DataSegment dataSegment)
  {
    return segmentLocks.compute(
        dataSegment,
        (segment, lock) -> {
          final ReferenceCountingLock nonNullLock;
          if (lock == null) {
            nonNullLock = new ReferenceCountingLock();
          } else {
            nonNullLock = lock;
          }
          nonNullLock.increment();
          return nonNullLock;
        }
    );
  }

  @SuppressWarnings("ObjectEquality")
  private void unlock(DataSegment dataSegment, ReferenceCountingLock lock)
  {
    segmentLocks.compute(
        dataSegment,
        (segment, existingLock) -> {
          if (existingLock == null) {
            throw new ISE("Lock has already been removed");
          } else if (existingLock != lock) {
            throw new ISE("Different lock instance");
          } else {
            if (existingLock.numReferences == 1) {
              return null;
            } else {
              existingLock.decrement();
              return existingLock;
            }
          }
        }
    );
  }

  private static class ReferenceCountingLock
  {
    private int numReferences;

    private void increment()
    {
      ++numReferences;
    }

    private void decrement()
    {
      --numReferences;
    }
  }

  @VisibleForTesting
  public ConcurrentHashMap<DataSegment, ReferenceCountingLock> getSegmentLocks()
  {
    return segmentLocks;
  }

  @VisibleForTesting
  public List<StorageLocation> getLocations()
  {
    return locations;
  }
}
