/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oharastream.ohara.metrics.kafka;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.metrics.BeanObject;

/** this class represents the topic metrics recorded by kafka. */
public class TopicMeter {
  // -------------------------[property keys]-------------------------//
  private static final String DOMAIN = "kafka.server";
  private static final String TYPE_KEY = "type";
  private static final String TYPE_VALUE = "BrokerTopicMetrics";
  private static final String TOPIC_KEY = "topic";
  private static final String NAME_KEY = "name";
  // -------------------------[attribute keys]-------------------------//
  private static final String COUNT_KEY = "Count";
  private static final String EVENT_TYPE_KEY = "EventType";
  private static final String FIFTEEN_MINUTE_RATE_KEY = "FifteenMinuteRate";
  private static final String FIVE_MINUTE_RATE_KEY = "FiveMinuteRate";
  private static final String MEAN_RATE_KEY = "MeanRate";
  private static final String ONE_MINUTE_RATE_KEY = "OneMinuteRate";
  private static final String RATE_UNIT_KEY = "RateUnit";

  public static Builder builder() {
    return new Builder();
  }

  /** reference to kafka.server.BrokerTopicStats */
  public enum Catalog {
    MessagesInPerSec,
    BytesInPerSec,
    BytesOutPerSec,
    BytesRejectedPerSec,
    ReplicationBytesInPerSec,
    ReplicationBytesOutPerSec,
    FailedProduceRequestsPerSec,
    FailedFetchRequestsPerSec,
    TotalProduceRequestsPerSec,
    TotalFetchRequestsPerSec,
    FetchMessageConversionsPerSec,
    ProduceMessageConversionsPerSec,
    ReassignmentBytesInPerSec,
    ReassignmentBytesOutPerSec
  }

  public static boolean is(BeanObject obj) {
    return obj.domainName().equals(DOMAIN)
        && TYPE_VALUE.equals(obj.properties().get(TYPE_KEY))
        && obj.properties().containsKey(TOPIC_KEY)
        && obj.properties().containsKey(NAME_KEY)
        && Stream.of(Catalog.values())
            .anyMatch(catalog -> catalog.name().equals(obj.properties().get(NAME_KEY)));
  }

  public static TopicMeter of(BeanObject obj) {
    return new TopicMeter(
        obj.properties().get(TOPIC_KEY),
        Catalog.valueOf(obj.properties().get(NAME_KEY)),
        // the metrics of kafka topic may be not ready and we all hate null. Hence, we fill some
        // "default value" to it.
        // the default value will be replaced by true value later.
        (long) obj.attributes().getOrDefault(COUNT_KEY, 0),
        (String) obj.attributes().getOrDefault(EVENT_TYPE_KEY, "unknown event"),
        (double) obj.attributes().getOrDefault(FIFTEEN_MINUTE_RATE_KEY, 0),
        (double) obj.attributes().getOrDefault(FIVE_MINUTE_RATE_KEY, 0),
        (double) obj.attributes().getOrDefault(MEAN_RATE_KEY, 0),
        (double) obj.attributes().getOrDefault(ONE_MINUTE_RATE_KEY, 0),
        (TimeUnit) obj.attributes().getOrDefault(RATE_UNIT_KEY, TimeUnit.SECONDS),
        obj.queryTime());
  }

  private final String topicName;
  private final Catalog catalog;
  private final long count;
  private final String eventType;
  private final double fifteenMinuteRate;
  private final double fiveMinuteRate;
  private final double meanRate;
  private final double oneMinuteRate;
  private final TimeUnit rateUnit;
  private final long queryTime;

  @VisibleForTesting
  TopicMeter(
      String topicName,
      Catalog catalog,
      long count,
      String eventType,
      double fifteenMinuteRate,
      double fiveMinuteRate,
      double meanRate,
      double oneMinuteRate,
      TimeUnit rateUnit,
      long queryTime) {
    this.topicName = CommonUtils.requireNonEmpty(topicName);
    this.catalog = Objects.requireNonNull(catalog);
    this.eventType = CommonUtils.requireNonEmpty(eventType);
    this.count = count;
    this.fifteenMinuteRate = fifteenMinuteRate;
    this.fiveMinuteRate = fiveMinuteRate;
    this.meanRate = meanRate;
    this.oneMinuteRate = oneMinuteRate;
    this.rateUnit = Objects.requireNonNull(rateUnit);
    this.queryTime = queryTime;
  }

  public String topicName() {
    return topicName;
  }

  public Catalog catalog() {
    return catalog;
  }

  public long count() {
    return count;
  }

  public String eventType() {
    return eventType;
  }

  public double fifteenMinuteRate() {
    return fifteenMinuteRate;
  }

  public double fiveMinuteRate() {
    return fiveMinuteRate;
  }

  public double meanRate() {
    return meanRate;
  }

  public double oneMinuteRate() {
    return oneMinuteRate;
  }

  public TimeUnit rateUnit() {
    return rateUnit;
  }

  public long queryTime() {
    return queryTime;
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<TopicMeter> {
    private String topicName;
    private Catalog catalog;
    private long count = 0;
    private String eventType = "N/A";
    private double fifteenMinuteRate = CommonUtils.randomDouble();
    private double fiveMinuteRate = CommonUtils.randomDouble();
    private double meanRate = CommonUtils.randomDouble();
    private double oneMinuteRate = CommonUtils.randomDouble();
    private TimeUnit rateUnit;
    private long queryTime = CommonUtils.current();

    private Builder() {}

    public Builder topicName(String topicName) {
      this.topicName = CommonUtils.requireNonEmpty(topicName);
      return this;
    }

    public Builder catalog(Catalog catalog) {
      this.catalog = Objects.requireNonNull(catalog);
      return this;
    }

    @Optional("default is zero")
    public Builder count(long count) {
      this.count = count;
      return this;
    }

    @Optional("default is N/A")
    public Builder eventType(String eventType) {
      this.eventType = CommonUtils.requireNonEmpty(eventType);
      return this;
    }

    @Optional("default is random long")
    public Builder fifteenMinuteRate(long fifteenMinuteRate) {
      this.fifteenMinuteRate = CommonUtils.requirePositiveLong(fifteenMinuteRate);
      return this;
    }

    @Optional("default is random long")
    public Builder fiveMinuteRate(long fiveMinuteRate) {
      this.fiveMinuteRate = CommonUtils.requirePositiveLong(fiveMinuteRate);
      return this;
    }

    @Optional("default is random long")
    public Builder meanRate(long meanRate) {
      this.meanRate = CommonUtils.requirePositiveLong(meanRate);
      return this;
    }

    @Optional("default is random long")
    public Builder oneMinuteRate(long oneMinuteRate) {
      this.oneMinuteRate = CommonUtils.requirePositiveLong(oneMinuteRate);
      return this;
    }

    public Builder rateUnit(TimeUnit rateUnit) {
      this.rateUnit = Objects.requireNonNull(rateUnit);
      return this;
    }

    @Optional("default is current time")
    public Builder queryTime(long queryTime) {
      this.queryTime = CommonUtils.requirePositiveLong(queryTime);
      return this;
    }

    /**
     * Create a topicMeter.
     *
     * @return topicMeter
     */
    @Override
    public TopicMeter build() {
      return new TopicMeter(
          topicName,
          catalog,
          count,
          eventType,
          fifteenMinuteRate,
          fiveMinuteRate,
          meanRate,
          oneMinuteRate,
          rateUnit,
          queryTime);
    }
  }
}
