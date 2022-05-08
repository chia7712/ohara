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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import oharastream.ohara.common.annotations.Nullable;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.setting.TopicKey;

/** A wrap to SourceRecord. Currently, only value columns and value are changed. */
public class RowSourceRecord {
  private final Map<String, ?> sourcePartition;
  private final Map<String, ?> sourceOffset;
  private final TopicKey topicKey;

  @Nullable("thanks to kafka")
  private final Integer partition;

  private final Row row;

  @Nullable("thanks to kafka")
  private final Long timestamp;

  private RowSourceRecord(
      Map<String, ?> sourcePartition,
      Map<String, ?> sourceOffset,
      TopicKey topicKey,
      Integer partition,
      Row row,
      Long timestamp) {
    this.sourcePartition = Collections.unmodifiableMap(Objects.requireNonNull(sourcePartition));
    this.sourceOffset = Collections.unmodifiableMap(Objects.requireNonNull(sourceOffset));
    this.topicKey = topicKey;
    this.partition = partition;
    this.row = row;
    this.timestamp = timestamp;
  }

  public Map<String, ?> sourcePartition() {
    return sourcePartition;
  }

  public Map<String, ?> sourceOffset() {
    return sourceOffset;
  }

  public TopicKey topicKey() {
    return topicKey;
  }

  public Optional<Integer> partition() {
    return Optional.ofNullable(partition);
  }

  public Row row() {
    return row;
  }

  public Optional<Long> timestamp() {
    return Optional.ofNullable(timestamp);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "RowSourceRecord{"
        + "sourcePartition="
        + sourcePartition
        + ", sourceOffset="
        + sourceOffset
        + ", topicKey="
        + topicKey
        + ", partition="
        + partition
        + ", row="
        + row
        + ", timestamp="
        + timestamp
        + '}';
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<RowSourceRecord> {
    private Builder() {
      // do nothing
    }

    private Map<String, ?> sourcePartition = Map.of();
    private Map<String, ?> sourceOffset = Map.of();
    private Integer partition = null;
    private Row row = null;
    private Long timestamp = null;
    private TopicKey topicKey = null;

    @oharastream.ohara.common.annotations.Optional("default is empty")
    public Builder sourcePartition(Map<String, ?> sourcePartition) {
      this.sourcePartition = Objects.requireNonNull(sourcePartition);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is empty")
    public Builder sourceOffset(Map<String, ?> sourceOffset) {
      this.sourceOffset = Objects.requireNonNull(sourceOffset);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional(
        "default is empty. It means target partition is computed by hash")
    public Builder partition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder row(Row row) {
      this.row = Objects.requireNonNull(row);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is current time")
    public Builder timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder topicKey(TopicKey topicKey) {
      this.topicKey = Objects.requireNonNull(topicKey);
      return this;
    }

    @Override
    public RowSourceRecord build() {
      return new RowSourceRecord(
          Objects.requireNonNull(sourcePartition),
          Objects.requireNonNull(sourceOffset),
          Objects.requireNonNull(topicKey),
          partition,
          Objects.requireNonNull(row),
          timestamp);
    }
  }
}
