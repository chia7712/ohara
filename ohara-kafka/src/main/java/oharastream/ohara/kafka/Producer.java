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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * a simple wrap from kafka producer.
 *
 * @param <Key> key type
 * @param <Value> value type
 */
public interface Producer<Key, Value> extends Releasable {

  /**
   * create a sender used to send a record to brokers
   *
   * @return a sender
   */
  Sender<Key, Value> sender();

  /** flush all on-the-flight data. */
  void flush();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>().keySerializer(Serializer.BYTES).valueSerializer(Serializer.BYTES);
  }

  class Builder<Key, Value>
      implements oharastream.ohara.common.pattern.Builder<Producer<Key, Value>> {
    private Map<String, String> options = Map.of();
    private String connectionProps;
    // default number of acks is 1.
    private short numberOfAcks = 1;
    private Serializer<?> keySerializer = null;
    private Serializer<?> valueSerializer = null;

    private Builder() {
      // no nothing
    }

    @Optional("default is empty")
    public Builder<Key, Value> option(String key, String value) {
      return options(Map.of(CommonUtils.requireNonEmpty(key), CommonUtils.requireNonEmpty(value)));
    }

    @Optional("default is empty")
    public Builder<Key, Value> options(Map<String, String> options) {
      this.options = CommonUtils.requireNonEmpty(options);
      return this;
    }

    public Builder<Key, Value> connectionProps(String connectionProps) {
      this.connectionProps = CommonUtils.requireNonEmpty(connectionProps);
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default number of acks is 1.")
    public Builder<Key, Value> noAcks() {
      this.numberOfAcks = 0;
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default number of acks is 1.")
    public Builder<Key, Value> allAcks() {
      this.numberOfAcks = -1;
      return this;
    }

    @SuppressWarnings("unchecked")
    public <NewKey> Builder<NewKey, Value> keySerializer(Serializer<NewKey> keySerializer) {
      this.keySerializer = Objects.requireNonNull(keySerializer);
      return (Builder<NewKey, Value>) this;
    }

    @SuppressWarnings("unchecked")
    public <newValue> Builder<Key, newValue> valueSerializer(Serializer<newValue> valueSerializer) {
      this.valueSerializer = Objects.requireNonNull(valueSerializer);
      return (Builder<Key, newValue>) this;
    }

    /**
     * Used to convert ohara row to byte array. It is a private class since ohara producer will
     * instantiate one and pass it to kafka producer. Hence, no dynamical call will happen in kafka
     * producer. The access exception won't be caused.
     *
     * @param serializer ohara serializer
     * @param <T> object type
     * @return a wrapper from kafka serializer
     */
    private static <T> org.apache.kafka.common.serialization.Serializer<T> wrap(
        Serializer<T> serializer) {
      return new org.apache.kafka.common.serialization.Serializer<T>() {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
          // do nothing
        }

        @Override
        public byte[] serialize(String topic, T data) {
          return data == null ? null : serializer.to(data);
        }

        @Override
        public void close() {
          // do nothing
        }
      };
    }

    private void checkArguments() {
      CommonUtils.requireNonEmpty(connectionProps);
      Objects.requireNonNull(keySerializer);
      Objects.requireNonNull(valueSerializer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Producer<Key, Value> build() {
      checkArguments();

      return new Producer<Key, Value>() {

        private Properties getProducerConfig() {
          Properties props = new Properties();
          options.forEach(props::setProperty);
          props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, connectionProps);
          props.setProperty(ProducerConfig.ACKS_CONFIG, String.valueOf(numberOfAcks));
          return props;
        }

        private final KafkaProducer<Key, Value> producer =
            new KafkaProducer<>(
                getProducerConfig(),
                wrap((Serializer<Key>) keySerializer),
                wrap((Serializer<Value>) valueSerializer));

        @Override
        public final Sender<Key, Value> sender() {
          return new Sender<Key, Value>() {
            @Override
            public CompletableFuture<RecordMetadata> doSend() {
              CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
              ProducerRecord<Key, Value> record =
                  new ProducerRecord<>(
                      topicKey.topicNameOnKafka(),
                      partition,
                      timestamp,
                      key,
                      value,
                      headers.stream()
                          .map(Builder.this::toKafkaHeader)
                          .collect(Collectors.toUnmodifiableList()));

              producer.send(
                  record,
                  (metadata, exception) -> {
                    if (metadata == null && exception == null)
                      completableFuture.completeExceptionally(
                          new IllegalStateException(
                              "no meta and exception from kafka producer...It should be impossible"));
                    if (metadata != null && exception != null)
                      completableFuture.completeExceptionally(
                          new IllegalStateException(
                              "Both meta and exception from kafka producer...It should be impossible"));
                    if (metadata != null) completableFuture.complete(RecordMetadata.of(metadata));
                    if (exception != null) completableFuture.completeExceptionally(exception);
                  });
              return completableFuture;
            }
          };
        }

        @Override
        public void flush() {
          producer.flush();
        }

        @Override
        public void close() {
          producer.close();
        }
      };
    }

    private org.apache.kafka.common.header.Header toKafkaHeader(Header header) {
      return new org.apache.kafka.common.header.Header() {
        @Override
        public String key() {
          return header.key();
        }

        @Override
        public byte[] value() {
          return header.value();
        }
      };
    }
  }

  /**
   * a fluent-style sender. kafak.ProducerRecord has many fields and most from them are nullable. It
   * makes kafak.ProducerRecord's constructor complicated. This class has fluent-style methods
   * helping user to fill the fields they have.
   */
  abstract class Sender<Key, Value> {
    protected Integer partition = null;
    protected List<Header> headers = List.of();
    protected Key key = null;
    protected Value value = null;
    protected Long timestamp = null;
    protected TopicKey topicKey = null;

    @VisibleForTesting
    Sender() {
      // do nothing
    }

    @Optional("default is hash of key")
    public Sender<Key, Value> partition(int partition) {
      this.partition = partition;
      return this;
    }

    @Optional("default is empty")
    public Sender<Key, Value> header(Header header) {
      return headers(List.of(Objects.requireNonNull(header)));
    }

    @Optional("default is empty")
    public Sender<Key, Value> headers(List<Header> headers) {
      this.headers = CommonUtils.requireNonEmpty(headers);
      return this;
    }

    @Optional("default is null")
    public Sender<Key, Value> key(Key key) {
      this.key = Objects.requireNonNull(key);
      return this;
    }

    @Optional("default is null")
    public Sender<Key, Value> value(Value value) {
      this.value = Objects.requireNonNull(value);
      return this;
    }

    @Optional("default is null")
    public Sender<Key, Value> timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Sender<Key, Value> topicKey(TopicKey topicKey) {
      this.topicKey = Objects.requireNonNull(topicKey);
      return this;
    }

    private void checkArguments() {
      Objects.requireNonNull(headers);
      Objects.requireNonNull(topicKey);
    }

    /**
     * start to send the data in background. Noted: you should check the returned future to handle
     * the exception or result
     *
     * @return an async thread processing the request
     */
    public CompletableFuture<RecordMetadata> send() {
      checkArguments();
      return doSend();
    }

    protected abstract CompletableFuture<RecordMetadata> doSend();
  }
}
