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

package oharastream.ohara.kafka.connector;

import java.util.Objects;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.TimestampType;

/**
 * The methods it have are almost same with SinkRecord. It return Table rather than any object.
 * Also, it doesn't have method to return value schema because the value schema is useless to user.
 */
public class RowSinkRecord {

  private final TopicKey topicKey;
  private final Row row;
  private final int partition;
  private final long offset;
  private final long timestamp;
  private final TimestampType timestampType;

  private RowSinkRecord(
      TopicKey topicKey,
      Row row,
      int partition,
      long offset,
      long timestamp,
      TimestampType timestampType) {
    this.topicKey = Objects.requireNonNull(topicKey);
    this.row = Objects.requireNonNull(row);
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.timestampType = Objects.requireNonNull(timestampType);
  }

  public TopicKey topicKey() {
    return topicKey;
  }

  public TopicPartition topicPartition() {
    return new TopicPartition(topicKey, partition);
  }

  public Row row() {
    return row;
  }

  public int partition() {
    return partition;
  }

  public long offset() {
    return offset;
  }

  public long timestamp() {
    return timestamp;
  }

  public TimestampType timestampType() {
    return timestampType;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<RowSinkRecord> {
    private Builder() {
      // do nothing
    }

    private TopicKey topicKey;
    private Row row;
    private Integer partition;
    private Long offset;
    private Long timestamp;
    private TimestampType timestampType;

    public Builder topicKey(TopicKey topicKey) {
      this.topicKey = Objects.requireNonNull(topicKey);
      return this;
    }

    public Builder row(Row row) {
      this.row = Objects.requireNonNull(row);
      return this;
    }

    public Builder partition(int partition) {
      this.partition = CommonUtils.requireNonNegativeInt(partition);
      return this;
    }

    public Builder offset(long offset) {
      this.offset = CommonUtils.requireNonNegativeLong(offset);
      return this;
    }

    public Builder timestamp(long timestamp) {
      this.timestamp = CommonUtils.requireNonNegativeLong(timestamp);
      return this;
    }

    public Builder timestampType(TimestampType timestampType) {
      this.timestampType = Objects.requireNonNull(timestampType);
      return this;
    }

    @Override
    public RowSinkRecord build() {
      return new RowSinkRecord(
          Objects.requireNonNull(topicKey),
          Objects.requireNonNull(row),
          Objects.requireNonNull(partition),
          Objects.requireNonNull(offset),
          Objects.requireNonNull(timestamp),
          Objects.requireNonNull(timestampType));
    }
  }
}
