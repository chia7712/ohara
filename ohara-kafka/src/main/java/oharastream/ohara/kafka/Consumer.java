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

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.kafka.connector.TopicPartition;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A wrap of kafka consumer.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Consumer<K, V> extends Releasable {

  /**
   * poll the data from subscribed topics
   *
   * @param timeout waiting time
   * @return records
   */
  List<Record<K, V>> poll(Duration timeout);

  /**
   * Overloading poll method
   *
   * @param timeout waiting time
   * @param expectedSize expected size
   * @return Records
   */
  default List<Record<K, V>> poll(Duration timeout, int expectedSize) {
    return poll(timeout, expectedSize, () -> false, Function.identity());
  }

  /**
   * It accept another condition - expected size from records. Somethins it is helpful if you
   * already know the number from records which should be returned.
   *
   * @param timeout timeout
   * @param expectedSize the number from records should be returned
   * @param stop supply a single to stop the internal loop
   * @param filter data filter
   * @return records
   */
  default List<Record<K, V>> poll(
      Duration timeout,
      int expectedSize,
      Supplier<Boolean> stop,
      Function<List<Record<K, V>>, List<Record<K, V>>> filter) {

    List<Record<K, V>> list;
    if (expectedSize == Integer.MAX_VALUE) list = new ArrayList<>();
    else list = new ArrayList<>(expectedSize);

    long endtime = CommonUtils.current() + timeout.toMillis();
    long ramaining = endtime - CommonUtils.current();

    while (!stop.get() && list.size() < expectedSize && ramaining > 0) {
      list.addAll(filter.apply(poll(Duration.ofMillis(ramaining))));
      ramaining = endtime - CommonUtils.current();
    }
    return list;
  }

  /** @return the topic names subscribed by this consumer */
  Set<String> subscription();

  /** @return Get the set of partitions currently assigned to this consumer. */
  Set<TopicPartition> assignment();

  /**
   * Seek to the first offset for each of the given partitions.
   *
   * @param partitions setting Partition list
   */
  void seekToBeginning(Collection<TopicPartition> partitions);

  /** Seek to the first offset for all partitions */
  default void seekToBeginning() {
    seekToBeginning(assignment());
  }

  /**
   * move the offset for all specific partition. if the offset is smaller or equal to zero, this
   * method is equal to {@link #seekToBeginning(Collection)}
   *
   * @param partition message partition
   * @param offset message offset
   */
  void seek(TopicPartition partition, long offset);

  /**
   * move the offset for all subscribed partitions
   *
   * @param offset message offset
   */
  default void seek(long offset) {
    assignment().forEach(p -> seek(p, offset));
  }

  /**
   * @return all partitions and offsets even if those partitions are not subscribed by this
   *     consumer. Noted that only ohara topics are listed
   */
  Map<TopicPartition, Long> endOffsets();

  /** break the poll right now. */
  void wakeup();

  /**
   * subscribe other topics. Noted that current assignments will be replaced by this new topics.
   *
   * @param topicKeys topic keys
   */
  void subscribe(Set<TopicKey> topicKeys);

  /**
   * subscribe other partitions. Noted that current assignments will be replaced by this new
   * partitions.
   *
   * @param assignments partitions
   */
  void assignments(Set<TopicPartition> assignments);

  /**
   * subscribe other partitions. Noted that current assignments will be replaced by this new
   * partitions.
   *
   * @param assignments partitions and offsets
   */
  void assignments(Map<TopicPartition, Long> assignments);

  static Builder<byte[], byte[]> builder() {
    return new Builder<>().keySerializer(Serializer.BYTES).valueSerializer(Serializer.BYTES);
  }

  class Builder<Key, Value>
      implements oharastream.ohara.common.pattern.Builder<Consumer<Key, Value>> {
    private Map<String, String> options = Map.of();
    private OffsetResetStrategy fromBegin = OffsetResetStrategy.LATEST;
    private Set<TopicKey> topicKeys;
    private Set<TopicPartition> assignments;
    private String groupId = String.format("ohara-consumer-%s", CommonUtils.randomString());
    private String connectionProps;
    private Serializer<?> keySerializer = null;
    private Serializer<?> valueSerializer = null;

    private Builder() {
      // do nothing
    }

    @oharastream.ohara.common.annotations.Optional("default is empty")
    public Builder<Key, Value> option(String key, String value) {
      return options(Map.of(CommonUtils.requireNonEmpty(key), CommonUtils.requireNonEmpty(value)));
    }

    @oharastream.ohara.common.annotations.Optional("default is empty")
    public Consumer.Builder<Key, Value> options(Map<String, String> options) {
      this.options = CommonUtils.requireNonEmpty(options);
      return this;
    }
    /**
     * receive all un-deleted message from subscribed topics
     *
     * @return this builder
     */
    @oharastream.ohara.common.annotations.Optional("default is OffsetResetStrategy.LATEST")
    public Builder<Key, Value> offsetFromBegin() {
      this.fromBegin = OffsetResetStrategy.EARLIEST;
      return this;
    }

    /**
     * receive the messages just after the last one
     *
     * @return this builder
     */
    @oharastream.ohara.common.annotations.Optional("default is OffsetResetStrategy.LATEST")
    public Builder<Key, Value> offsetAfterLatest() {
      this.fromBegin = OffsetResetStrategy.LATEST;
      return this;
    }

    /**
     * @param topicKey the topic you want to subscribe
     * @return this builder
     */
    public Builder<Key, Value> topicKey(TopicKey topicKey) {
      return topicKeys(Set.of(Objects.requireNonNull(topicKey)));
    }

    /**
     * assign the specify topics to this consumer. You have to define either topicNames or
     * assignments.
     *
     * @param topicKeys the topics you want to subscribe
     * @return this builder
     */
    public Builder<Key, Value> topicKeys(Set<TopicKey> topicKeys) {
      if (assignments != null)
        throw new IllegalArgumentException("assignments is defined so you can't subscribe topics");
      this.topicKeys = CommonUtils.requireNonEmpty(topicKeys);
      return this;
    }

    /**
     * assign the specify partitions to this consumer. You have to define either topicNames or
     * assignments.
     *
     * @param assignments subscribed partitions
     * @return this builder
     */
    public Builder<Key, Value> assignments(Set<TopicPartition> assignments) {
      if (topicKeys != null)
        throw new IllegalArgumentException("assignments is defined so you can't subscribe topics");
      this.assignments = CommonUtils.requireNonEmpty(assignments);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is random string")
    public Builder<Key, Value> groupId(String groupId) {
      this.groupId = Objects.requireNonNull(groupId);
      return this;
    }

    public Builder<Key, Value> connectionProps(String connectionProps) {
      this.connectionProps = CommonUtils.requireNonEmpty(connectionProps);
      return this;
    }

    @SuppressWarnings("unchecked")
    public <NewKey> Builder<NewKey, Value> keySerializer(Serializer<NewKey> keySerializer) {
      this.keySerializer = Objects.requireNonNull(keySerializer);
      return (Builder<NewKey, Value>) this;
    }

    @SuppressWarnings("unchecked")
    public <NewValue> Builder<Key, NewValue> valueSerializer(Serializer<NewValue> valueSerializer) {
      this.valueSerializer = Objects.requireNonNull(valueSerializer);
      return (Builder<Key, NewValue>) this;
    }

    /**
     * Used to convert byte array to ohara row. It is a private class since ohara consumer will
     * instantiate one and pass it to kafka consumer. Hence, no dynamical call will happen in kafka
     * consumer. The access exception won't be caused.
     *
     * @param serializer ohara serializer
     * @return a wrapper from kafka deserializer
     */
    private static <T> Deserializer<T> wrap(Serializer<T> serializer) {
      return new org.apache.kafka.common.serialization.Deserializer<T>() {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
          // do nothing
        }

        @Override
        public T deserialize(String topic, byte[] data) {
          return data == null ? null : serializer.from(data);
        }

        @Override
        public void close() {
          // do nothing
        }
      };
    }

    private void checkArguments() {
      CommonUtils.requireNonEmpty(connectionProps);
      CommonUtils.requireNonEmpty(groupId);
      Objects.requireNonNull(fromBegin);
      Objects.requireNonNull(keySerializer);
      Objects.requireNonNull(valueSerializer);
    }

    private static org.apache.kafka.common.TopicPartition toKafka(TopicPartition tp) {
      return new org.apache.kafka.common.TopicPartition(
          tp.topicKey().topicNameOnKafka(), tp.partition());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Consumer<Key, Value> build() {
      checkArguments();

      Properties props = new Properties();
      options.forEach(props::setProperty);
      props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, connectionProps);
      props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      // kafka demand us to pass lowe case words...
      props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBegin.name().toLowerCase());

      KafkaConsumer<Key, Value> kafkaConsumer =
          new KafkaConsumer<>(
              props,
              wrap((Serializer<Key>) keySerializer),
              wrap((Serializer<Value>) valueSerializer));

      if (!CommonUtils.isEmpty(topicKeys))
        kafkaConsumer.subscribe(
            topicKeys.stream()
                .map(TopicKey::topicNameOnKafka)
                .collect(Collectors.toUnmodifiableSet()));
      if (!CommonUtils.isEmpty(assignments))
        kafkaConsumer.assign(
            assignments.stream().map(Builder::toKafka).collect(Collectors.toUnmodifiableList()));

      return new Consumer<Key, Value>() {
        @Override
        public void close() {
          kafkaConsumer.close();
        }

        @Override
        public List<Record<Key, Value>> poll(Duration timeout) {
          ConsumerRecords<Key, Value> r = kafkaConsumer.poll(timeout);

          if (r == null || r.isEmpty()) return List.of();
          else
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(r.iterator(), Spliterator.ORDERED), false)
                .map(
                    cr ->
                        new Record<>(
                            TopicKey.requirePlain(cr.topic()),
                            cr.partition(),
                            cr.timestamp(),
                            TimestampType.of(cr.timestampType()),
                            cr.offset(),
                            Optional.ofNullable(cr.headers())
                                .map(
                                    headers ->
                                        StreamSupport.stream(headers.spliterator(), false)
                                            .map(header -> new Header(header.key(), header.value()))
                                            .collect(Collectors.toUnmodifiableList()))
                                .orElse(List.of()),
                            cr.key(),
                            cr.value()))
                .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public Set<String> subscription() {
          return Collections.unmodifiableSet(kafkaConsumer.subscription());
        }

        @Override
        public Set<TopicPartition> assignment() {
          return kafkaConsumer.assignment().stream()
              // remove non-ohara topics
              .filter(p -> TopicKey.ofPlain(p.topic()).isPresent())
              .map(TopicPartition::of)
              .collect(Collectors.toUnmodifiableSet());
        }

        @Override
        public void seekToBeginning(Collection<TopicPartition> partitions) {
          kafkaConsumer.seekToBeginning(
              partitions.stream().map(Builder::toKafka).collect(Collectors.toUnmodifiableList()));
        }

        @Override
        public void seek(TopicPartition partition, long offset) {
          kafkaConsumer.seek(Builder.toKafka(partition), Math.max(0, offset));
        }

        @Override
        public Map<TopicPartition, Long> endOffsets() {
          return kafkaConsumer
              .endOffsets(
                  kafkaConsumer.listTopics().entrySet().stream()
                      // remove non-ohara topics
                      .filter(e -> TopicKey.ofPlain(e.getKey()).isPresent())
                      .flatMap(
                          e ->
                              e.getValue().stream()
                                  .map(
                                      p ->
                                          new org.apache.kafka.common.TopicPartition(
                                              p.topic(), p.partition())))
                      .collect(Collectors.toUnmodifiableList()))
              .entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      e -> TopicPartition.of(e.getKey()), Map.Entry::getValue));
        }

        @Override
        public void wakeup() {
          kafkaConsumer.wakeup();
        }

        @Override
        public void subscribe(Set<TopicKey> topicKeys) {
          kafkaConsumer.subscribe(
              topicKeys.stream()
                  .map(TopicKey::topicNameOnKafka)
                  .collect(Collectors.toUnmodifiableList()));
        }

        @Override
        public void assignments(Set<TopicPartition> assignments) {
          kafkaConsumer.assign(
              assignments.stream().map(Builder::toKafka).collect(Collectors.toUnmodifiableList()));
        }

        @Override
        public void assignments(Map<TopicPartition, Long> assignments) {
          assignments(assignments.keySet());
          assignments.forEach(
              (tp, offset) -> kafkaConsumer.seek(Builder.toKafka(tp), Math.max(0, offset)));
        }
      };
    }
  }

  /**
   * a scala wrap from kafka's consumer record.
   *
   * @param <K> K key type
   * @param <V> V value type
   */
  class Record<K, V> {
    private final TopicKey topicKey;
    private final int partition;
    private final long timestamp;
    private final TimestampType timestampType;
    private final long offset;
    private final List<Header> headers;
    private final K key;
    private final V value;

    /**
     * @param topicKey topic name
     * @param timestamp time to create this record or time to append this record.
     * @param key key (nullable)
     * @param value value
     */
    private Record(
        TopicKey topicKey,
        int partition,
        long timestamp,
        TimestampType timestampType,
        long offset,
        List<Header> headers,
        K key,
        V value) {
      this.topicKey = Objects.requireNonNull(topicKey);
      this.partition = partition;
      this.timestamp = timestamp;
      this.timestampType = timestampType;
      this.offset = offset;
      this.headers = Collections.unmodifiableList(headers);
      this.key = key;
      this.value = value;
    }

    /**
     * Kafka topic name
     *
     * @return a topic name
     */
    public TopicKey topicKey() {
      return topicKey;
    }

    public int partition() {
      return partition;
    }
    /**
     * The timestamp of this record.
     *
     * @return timestamp
     */
    public long timestamp() {
      return timestamp;
    }

    /**
     * The timestamp type of this record
     *
     * @return timestamp type
     */
    public TimestampType timestampType() {
      return timestampType;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     *
     * @return offset
     */
    public long offset() {
      return offset;
    }

    /**
     * The headers
     *
     * @return header list
     */
    public List<Header> headers() {
      return headers;
    }

    /**
     * The key
     *
     * @return optional key
     */
    public Optional<K> key() {
      return Optional.ofNullable(key);
    }

    /**
     * The value
     *
     * @return optional value
     */
    public Optional<V> value() {
      return Optional.ofNullable(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Record<?, ?> that = (Record<?, ?>) o;
      return Objects.equals(topicKey, that.topicKey)
          && Objects.equals(timestamp, that.timestamp)
          && Objects.equals(timestampType, that.timestampType)
          && Objects.equals(offset, that.offset)
          && CommonUtils.equals(headers, that.headers)
          && Objects.equals(key, that.key)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(topicKey, headers, key, value);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("topicKey", topicKey)
          .append("timestamp", timestamp)
          .append("offset", offset)
          .append("headers", headers)
          .append("key", key)
          .append("value", value)
          .toString();
    }
  }
}
