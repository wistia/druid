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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.seekablestream.extension.KafkaConfigOverrides;
import org.apache.druid.indexing.seekablestream.supervisor.IdleConfig;
import org.apache.druid.indexing.seekablestream.supervisor.LagAggregator;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Map;

public class KafkaSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
{
  public static final String DRUID_DYNAMIC_CONFIG_PROVIDER_KEY = "druid.dynamic.config.provider";
  public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  public static final String TRUST_STORE_PASSWORD_KEY = "ssl.truststore.password";
  public static final String KEY_STORE_PASSWORD_KEY = "ssl.keystore.password";
  public static final String KEY_PASSWORD_KEY = "ssl.key.password";
  public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

  public static final boolean DEFAULT_IS_MULTI_TOPIC = false;

  private final Map<String, Object> consumerProperties;
  private final long pollTimeout;
  private final KafkaConfigOverrides configOverrides;
  private final String topic;
  private final String topicPattern;
  private final boolean emitTimeLagMetrics;

  @JsonCreator
  public KafkaSupervisorIOConfig(
      @JsonProperty("topic") String topic,
      @JsonProperty("topicPattern") String topicPattern,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @Nullable @JsonProperty("autoScalerConfig") AutoScalerConfig autoScalerConfig,
      @Nullable @JsonProperty("lagAggregator") LagAggregator lagAggregator,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("lateMessageRejectionStartDateTime") DateTime lateMessageRejectionStartDateTime,
      @JsonProperty("configOverrides") KafkaConfigOverrides configOverrides,
      @JsonProperty("idleConfig") IdleConfig idleConfig,
      @JsonProperty("stopTaskCount") Integer stopTaskCount,
      @Nullable @JsonProperty("emitTimeLagMetrics") Boolean emitTimeLagMetrics
  )
  {
    super(
        checkTopicArguments(topic, topicPattern),
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        startDelay,
        period,
        useEarliestOffset,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        autoScalerConfig,
        Configs.valueOrDefault(lagAggregator, LagAggregator.DEFAULT),
        lateMessageRejectionStartDateTime,
        idleConfig,
        stopTaskCount
    );

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    Preconditions.checkNotNull(
        consumerProperties.get(BOOTSTRAP_SERVERS_KEY),
        StringUtils.format("consumerProperties must contain entry for [%s]", BOOTSTRAP_SERVERS_KEY)
    );
    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
    this.configOverrides = configOverrides;
    this.topic = topic;
    this.topicPattern = topicPattern;
    this.emitTimeLagMetrics = Configs.valueOrDefault(emitTimeLagMetrics, false);
  }

  /**
   * Only used in testing or serialization/deserialization
   */
  @JsonProperty
  public String getTopic()
  {
    return topic;
  }

  /**
   * Only used in testing or serialization/deserialization
   */
  @JsonProperty
  public String getTopicPattern()
  {
    return topicPattern;
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public boolean isUseEarliestOffset()
  {
    return isUseEarliestSequenceNumber();
  }

  @JsonProperty
  public KafkaConfigOverrides getConfigOverrides()
  {
    return configOverrides;
  }

  public boolean isMultiTopic()
  {
    return topicPattern != null;
  }

  /**
   * @return true if supervisor needs to publish the time lag.
   */
  @JsonProperty
  public boolean isEmitTimeLagMetrics()
  {
    return emitTimeLagMetrics;
  }

  @Override
  public String toString()
  {
    return "KafkaSupervisorIOConfig{" +
           "topic='" + getTopic() + '\'' +
           "topicPattern='" + getTopicPattern() + '\'' +
           ", replicas=" + getReplicas() +
           ", taskCount=" + getTaskCount() +
           ", taskDuration=" + getTaskDuration() +
           ", consumerProperties=" + consumerProperties +
           ", autoScalerConfig=" + getAutoScalerConfig() +
           ", pollTimeout=" + pollTimeout +
           ", startDelay=" + getStartDelay() +
           ", period=" + getPeriod() +
           ", useEarliestOffset=" + isUseEarliestOffset() +
           ", completionTimeout=" + getCompletionTimeout() +
           ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
           ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
           ", lateMessageRejectionStartDateTime=" + getLateMessageRejectionStartDateTime() +
           ", configOverrides=" + getConfigOverrides() +
           ", idleConfig=" + getIdleConfig() +
           ", stopTaskCount=" + getStopTaskCount() +
           '}';
  }

  private static String checkTopicArguments(String topic, String topicPattern)
  {
    if (topic == null && topicPattern == null) {
      throw InvalidInput.exception("Either topic or topicPattern must be specified");
    }
    if (topic != null && topicPattern != null) {
      throw InvalidInput.exception(
          "Only one of topic [%s] or topicPattern [%s] must be specified",
          topic,
          topicPattern
      );
    }
    return topic != null ? topic : topicPattern;
  }

}
