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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkTaskContext;

/** a wrap to kafka SinkTaskContext */
public interface RowSinkContext {
  /**
   * Reset the consumer offsets for the given topic partitions. SinkTasks should use this if they
   * manage offsets in the sink data store rather than using Kafka consumer offsets. For example, an
   * HDFS connector might record offsets in HDFS to provide exactly once delivery. When the SinkTask
   * is started or a rebalance occurs, the task would reload offsets from HDFS and use this method
   * to reset the consumer to those offsets.
   *
   * <p>SinkTasks that do not manage their own offsets do not need to use this method.
   *
   * @param offsets map from offsets for topic partitions
   */
  void offset(Map<TopicPartition, Long> offsets);

  /**
   * Get the current set of assigned TopicPartitions for this task.
   *
   * @return the set of currently assigned TopicPartitions
   */
  Set<TopicPartition> assignment();

  /**
   * Reset the consumer offsets for the given topic partition. SinkTasks should use if they manage
   * offsets in the sink data store rather than using Kafka consumer offsets. For example, an HDFS
   * connector might record offsets in HDFS to provide exactly once delivery. When the topic
   * partition is recovered the task would reload offsets from HDFS and use this method to reset the
   * consumer to the offset.
   *
   * <p>SinkTasks that do not manage their own offsets do not need to use this method.
   *
   * @param partition the topic partition to reset offset.
   * @param offset the offset to reset to.
   */
  default void offset(TopicPartition partition, long offset) {
    this.offset(Map.of(partition, offset));
  }

  static RowSinkContext toRowSinkContext(SinkTaskContext context) {
    return new RowSinkContext() {
      @Override
      public void offset(Map<TopicPartition, Long> offsets) {
        context.offset(
            offsets.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        entry ->
                            new org.apache.kafka.common.TopicPartition(
                                entry.getKey().topicKey().topicNameOnKafka(),
                                entry.getKey().partition()),
                        Map.Entry::getValue)));
      }

      @Override
      public Set<TopicPartition> assignment() {
        return context.assignment().stream()
            .map(TopicPartition::of)
            .collect(Collectors.toUnmodifiableSet());
      }
    };
  }
}
