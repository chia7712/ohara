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

package oharastream.ohara.stream.ostream;

import java.util.HashMap;
import java.util.Map;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.metrics.basic.Counter;
import oharastream.ohara.stream.metric.MetricFactory;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

/**
 * This is a abstract super class of {@code OStream}, {@code OTable} and {@code OGroupStream}. We
 * use this class to pass kafka inner constructor objects in order to reduce the dependencies of
 * kafka.
 *
 * @param <K> key type of source stream
 * @param <V> value type of source stream
 */
@SuppressWarnings({"unchecked"})
abstract class AbstractStream<K, V> {
  private static final Map<ObjectKey, Counter> TOPIC_IN_COUNTERS = new HashMap<>();
  private static final Map<ObjectKey, Counter> TOPIC_OUT_COUNTERS = new HashMap<>();

  /**
   * get or create global counter to record metrics for specific topic
   *
   * @param key object key of this stream
   * @param type topic type
   * @return an new counter. Or existent count
   */
  static Counter counter(ObjectKey key, MetricFactory.IOType type) {
    final Map<ObjectKey, Counter> counters;
    switch (type) {
      case TOPIC_OUT:
        counters = TOPIC_IN_COUNTERS;
        break;
      case TOPIC_IN:
        counters = TOPIC_OUT_COUNTERS;
        break;
      default:
        throw new RuntimeException("unknown type:" + type);
    }
    synchronized (counters) {
      Counter counter = counters.get(key);
      if (counter == null) {
        counter = MetricFactory.getCounter(key, type);
        counters.put(key, counter);
      }
      return counter;
    }
  }

  KTable<K, V> ktable;
  KStream<K, V> kstreams;
  KGroupedStream<K, V> kgroupstream;
  final OStreamBuilder builder;
  final StreamsBuilder innerBuilder;
  private final Counter counter;

  @SuppressWarnings("unchecked")
  AbstractStream(final OStreamBuilder builder) {
    this.counter = counter(builder.key(), MetricFactory.IOType.TOPIC_IN);
    StreamsBuilder newBuilder = new StreamsBuilder();
    this.kstreams =
        newBuilder.stream(builder.getFromTopic(), builder.getFromSerde().get())
            // since the value is "byte array" of nothing, we only care the key part, i.e, the real
            // row data.
            // here we convert the row data to both the key and value part
            // TODO : is there any good way to avoid this ugly convert?...by Sam
            .map(
                (key, value) -> {
                  // we calculate the input record size
                  counter.incrementAndGet();
                  return KeyValue.pair(key, key);
                });
    this.builder = builder;
    this.innerBuilder = newBuilder;
  }

  AbstractStream(
      final OStreamBuilder builder, final KStream<K, V> kstreams, StreamsBuilder innerBuilder) {
    this.counter = counter(builder.key(), MetricFactory.IOType.TOPIC_IN);
    this.builder = builder;
    this.kstreams = kstreams;
    this.innerBuilder = innerBuilder;
  }

  AbstractStream(
      final OStreamBuilder builder,
      final KGroupedStream<K, V> kgroupstream,
      StreamsBuilder innerBuilder) {
    this.counter = counter(builder.key(), MetricFactory.IOType.TOPIC_IN);
    this.builder = builder;
    this.kgroupstream = kgroupstream;
    this.innerBuilder = innerBuilder;
  }

  AbstractStream(
      final OStreamBuilder builder, final KTable<K, V> ktable, StreamsBuilder innerBuilder) {
    this.counter = counter(builder.key(), MetricFactory.IOType.TOPIC_IN);
    this.builder = builder;
    this.ktable = ktable;
    this.innerBuilder = innerBuilder;
  }
}
