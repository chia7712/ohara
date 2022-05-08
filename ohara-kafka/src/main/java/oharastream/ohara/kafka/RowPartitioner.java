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

package oharastream.ohara.kafka;

import java.util.Map;
import java.util.Optional;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.setting.WithDefinitions;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;

/**
 * The partition distribution is up to RowPartitioner. Implementing custom RowPartitioner is able to
 * define the distribution of data and you can supply the custom balancer strategy.
 *
 * <p>By default, the distribution is calculated by hash.
 */
public abstract class RowPartitioner implements Partitioner, WithDefinitions {

  private final DefaultPartitioner kafkaDefaultPartitioner = new DefaultPartitioner();

  /**
   * Compute the partition for the given record. Noted: if the input data is NOT row, the partition
   * distribution is calculated by default implementation.
   *
   * @param topicKey The topic key
   * @param row The row data
   * @param serializedRow row in bytes array
   * @param cluster The current cluster metadata
   * @return the number of partition to store the data. Or empty if you have no idea :)
   */
  public Optional<Integer> partition(
      TopicKey topicKey, Row row, byte[] serializedRow, Cluster cluster) {
    return Optional.empty();
  }

  // -----------------------------[wrap]-----------------------------//

  @Override
  public final int partition(
      String topic,
      Object key,
      byte[] keyBytes,
      Object value,
      byte[] valueBytes,
      org.apache.kafka.common.Cluster cluster) {
    // if the input data is NOT ohara's data, we don't pass it to following partitioner.
    return TopicKey.ofPlain(topic)
        .flatMap(
            topicKey ->
                key instanceof Row ? Optional.of(Map.entry(topicKey, (Row) key)) : Optional.empty())
        .flatMap(pair -> partition(pair.getKey(), pair.getValue(), keyBytes, Cluster.of(cluster)))
        .orElseGet(
            () ->
                kafkaDefaultPartitioner.partition(
                    topic, key, keyBytes, value, valueBytes, cluster));
  }

  // TODO: should we open them to ohara developer ???
  // -----------------------------[internal]-----------------------------//
  @Override
  public final void configure(Map<String, ?> configs) {
    kafkaDefaultPartitioner.configure(configs);
  }

  @Override
  public final void close() {
    kafkaDefaultPartitioner.close();
  }

  @Override
  public final void onNewBatch(
      String topic, org.apache.kafka.common.Cluster cluster, int prevPartition) {
    kafkaDefaultPartitioner.onNewBatch(topic, cluster, prevPartition);
  }
}
