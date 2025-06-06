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

package org.apache.druid.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mimics the behavior of {@link org.apache.druid.client.CachingClusteredClient} when it queries data servers (like
 * Historicals, which use {@link org.apache.druid.server.coordination.ServerManager}). Used by {@link QueryStackTests}.
 *
 * This class's logic is like a mashup of those two classes. With the right abstractions, it may be possible to get rid
 * of this class and replace it with the production classes.
 */
public class TestClusterQuerySegmentWalker implements QuerySegmentWalker
{
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>> timelines;
  private final QueryRunnerFactoryConglomerate conglomerate;
  @Nullable
  private final QueryScheduler scheduler;
  private final EtagProvider etagProvider;

  public static class TestSegmentsBroker
  {
    public final Map<String, VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>> timelines = new HashMap<>();
  }

  @Inject
  TestClusterQuerySegmentWalker(
      TestSegmentsBroker testSegmentsBroker,
      QueryRunnerFactoryConglomerate conglomerate,
      @Nullable QueryScheduler scheduler,
      EtagProvider etagProvider)
  {
    this(testSegmentsBroker.timelines, conglomerate, scheduler, etagProvider);
  }

  TestClusterQuerySegmentWalker(
      Map<String, VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>> timelines,
      QueryRunnerFactoryConglomerate conglomerate,
      @Nullable QueryScheduler scheduler,
      EtagProvider etagProvider)
  {
    this.timelines = timelines;
    this.conglomerate = conglomerate;
    this.scheduler = scheduler;
    this.etagProvider = etagProvider;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    // Just like CachingClusteredClient, ignore "query" and defer action until the QueryRunner is called.
    // Strange, but true. Required to get authentic behavior with UnionDataSources. (Although, it would be great if
    // this wasn't required.)
    return (queryPlus, responseContext) -> {
      ExecutionVertex ev = ExecutionVertex.of(queryPlus.getQuery());

      if (!(ev.isProcessable() && ev.isTableBased())) {
        throw new ISE("Cannot handle datasource: %s", queryPlus.getQuery().getDataSource());
      }

      final String dataSourceName = ev.getBaseTableDataSource().getName();

      FunctionalIterable<SegmentDescriptor> segmentDescriptors = FunctionalIterable
          .create(intervals)
          .transformCat(interval -> getSegmentsForTable(dataSourceName, interval))
          .transform(WindowedSegment::getDescriptor);

      return getQueryRunnerForSegments(queryPlus.getQuery(), segmentDescriptors).run(queryPlus, responseContext);
    };
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    final DataSource dataSourceFromQuery = query.getDataSource();
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    ExecutionVertex ev = ExecutionVertex.of(query);

    if (!ev.canRunQueryUsingClusterWalker()) {
      throw new ISE("Cannot handle datasource: %s", dataSourceFromQuery);
    }

    final String dataSourceName = ev.getBaseTableDataSource().getName();

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    // Make sure this query type can handle the subquery, if present.
    if ((dataSourceFromQuery instanceof QueryDataSource)
        && !toolChest.canPerformSubquery(((QueryDataSource) dataSourceFromQuery).getQuery())) {
      throw new ISE("Cannot handle subquery: %s", dataSourceFromQuery);
    }

    final SegmentMapFunction segmentMapFn = ev.createSegmentMapFunction(NoopPolicyEnforcer.instance());

    final QueryRunner<T> baseRunner = new FinalizeResultsQueryRunner<>(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                toolChest.preMergeQueryDecoration(
                    (queryPlus, responseContext) -> {
                      return makeTableRunner(toolChest, factory, getSegmentsForTable(dataSourceName, specs), segmentMapFn)
                          .run(GroupByQueryRunnerTestHelper.populateResourceId(queryPlus), responseContext);
                    }
                ),
                false
            )
        ),
        toolChest
    );


    // Wrap baseRunner in a runner that rewrites the QuerySegmentSpec to mention the specific segments.
    // This mimics what CachingClusteredClient on the Broker does, and is required for certain queries (like Scan)
    // to function properly. SegmentServerSelector does not currently mimic CachingClusteredClient, it is using
    // the LocalQuerySegmentWalker constructor instead since this walker does not mimic remote DruidServer objects
    // to actually serve the queries
    return (theQuery, responseContext) -> {
      QueryPlus<T> newQuery = GroupByQueryRunnerTestHelper.populateResourceId(theQuery);
      responseContext.initializeRemainingResponses();

      String etag = etagProvider.getEtagFor(newQuery.getQuery());
      if (etag != null) {
        responseContext.put(Keys.ETAG, etag);
      }
      responseContext.addRemainingResponse(
          newQuery.getQuery().getMostSpecificId(), 0);

      if (scheduler != null) {
        Set<SegmentServerSelector> segments = new HashSet<>();
        specs.forEach(spec -> segments.add(new SegmentServerSelector(spec)));
        return scheduler.run(
            scheduler.prioritizeAndLaneQuery(newQuery, segments),
            new LazySequence<>(
                () -> baseRunner.run(
                    newQuery.withQuery(Queries.withSpecificSegments(
                        newQuery.getQuery(),
                        ImmutableList.copyOf(specs)
                    )),
                    responseContext
                )
            )
        );
      } else {
        return baseRunner.run(
            newQuery.withQuery(Queries.withSpecificSegments(newQuery.getQuery(), ImmutableList.copyOf(specs))),
            responseContext
        );
      }
    };
  }

  private <T> QueryRunner<T> makeTableRunner(
      final QueryToolChest<T, Query<T>> toolChest,
      final QueryRunnerFactory<T, Query<T>> factory,
      final Iterable<WindowedSegment> segments,
      final SegmentMapFunction segmentMapFn
  )
  {
    final List<WindowedSegment> segmentsList = Lists.newArrayList(segments);

    if (segmentsList.isEmpty()) {
      // Note: this is not correct when there's a right or full outer join going on.
      // See https://github.com/apache/druid/issues/9229 for details.
      return new NoopQueryRunner<>();
    }

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
      {
        final Closer closer = Closer.create();
        try {
          QueryRunner<T> runner = new FinalizeResultsQueryRunner<>(
              toolChest.mergeResults(
                  factory.mergeRunners(
                      DirectQueryProcessingPool.INSTANCE,
                      FunctionalIterable
                          .create(segmentsList)
                          .transform(
                              segment ->
                                  new SpecificSegmentQueryRunner<>(
                                      factory.createRunner(closer.register(segmentMapFn.apply(segment.getSegment())
                                                                                       .orElseThrow())),
                                      new SpecificSegmentSpec(segment.getDescriptor())
                                  )
                          )
                  ),
                  true
              ),
              toolChest
          );
          return runner.run(queryPlus, responseContext).withBaggage(closer);
        }
        catch (Throwable e) {
          throw CloseableUtils.closeAndWrapInCatch(e, closer);
        }
      }
    };
  }

  private List<WindowedSegment> getSegmentsForTable(final String dataSource, final Interval interval)
  {
    final VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider> timeline = timelines.get(dataSource);

    if (timeline == null) {
      return Collections.emptyList();
    } else {
      final List<WindowedSegment> retVal = new ArrayList<>();

      for (TimelineObjectHolder<String, ReferenceCountedSegmentProvider> holder : timeline.lookup(interval)) {
        for (PartitionChunk<ReferenceCountedSegmentProvider> chunk : holder.getObject()) {
          retVal.add(new WindowedSegment(chunk.getObject(), holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()));
        }
      }

      return retVal;
    }
  }

  private List<WindowedSegment> getSegmentsForTable(final String dataSource, final Iterable<SegmentDescriptor> specs)
  {
    final VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider> timeline = timelines.get(dataSource);

    if (timeline == null) {
      return Collections.emptyList();
    } else {
      final List<WindowedSegment> retVal = new ArrayList<>();

      for (SegmentDescriptor spec : specs) {
        final PartitionChunk<ReferenceCountedSegmentProvider> entry = timeline.findChunk(
            spec.getInterval(),
            spec.getVersion(),
            spec.getPartitionNumber()
        );
        retVal.add(new WindowedSegment(entry.getObject(), spec.getInterval(), spec.getVersion(), spec.getPartitionNumber()));
      }

      return retVal;
    }
  }

  private static class WindowedSegment
  {
    private final ReferenceCountedSegmentProvider segment;
    private final Interval interval;
    private final String version;
    private final int partitionNumber;

    public WindowedSegment(ReferenceCountedSegmentProvider segment, Interval interval, String version, int partitionNumber)
    {
      if (segment.getBaseSegment().getId() != null) {
        Preconditions.checkArgument(segment.getBaseSegment().getId().getInterval().contains(interval));
      } else {
        Preconditions.checkArgument(
            segment.getBaseSegment().getDataInterval().contains(interval),
            "Data interval for non-table segment should default to external"
        );
      }
      this.segment = segment;
      this.interval = interval;
      this.version = version;
      this.partitionNumber = partitionNumber;
    }

    public ReferenceCountedSegmentProvider getSegment()
    {
      return segment;
    }

    public Interval getInterval()
    {
      return interval;
    }

    public SegmentDescriptor getDescriptor()
    {
      return new SegmentDescriptor(interval, version, partitionNumber);
    }
  }
}
