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

import java.util.Objects;
import java.util.function.Supplier;
import oharastream.ohara.common.setting.TopicKey;

public class RecordMetadata {
  /**
   * convert kafka.RecordMetadata to ohara.RecordMetadata
   *
   * @param metadata kafka.RecordMetadata
   * @return ohara.RecordMetadata
   */
  public static RecordMetadata of(org.apache.kafka.clients.producer.RecordMetadata metadata) {
    return new RecordMetadata(
        () -> TopicKey.requirePlain(metadata.topic()),
        metadata.partition(),
        metadata.offset(),
        metadata.timestamp(),
        metadata.serializedKeySize(),
        metadata.serializedValueSize());
  }

  /** It is expensive to parse topic key from json string so we use lazy initialization. */
  private final Supplier<TopicKey> topicKeySupplier;

  private TopicKey topicKey = null;
  private final int partition;
  private final long offset;
  private final long timestamp;
  private final int serializedKeySize;
  private final int serializedValueSize;

  private RecordMetadata(
      Supplier<TopicKey> topicKeySupplier,
      int partition,
      long offset,
      long timestamp,
      int serializedKeySize,
      int serializedValueSize) {
    this.topicKeySupplier = Objects.requireNonNull(topicKeySupplier);
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.serializedKeySize = serializedKeySize;
    this.serializedValueSize = serializedValueSize;
  }

  public TopicKey topicKey() {
    if (topicKey == null) topicKey = topicKeySupplier.get();
    return topicKey;
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

  public int serializedKeySize() {
    return serializedKeySize;
  }

  public int serializedValueSize() {
    return serializedValueSize;
  }
}
