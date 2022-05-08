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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.Consumer;
import oharastream.ohara.kafka.Producer;
import oharastream.ohara.kafka.TopicAdmin;
import oharastream.ohara.metrics.BeanChannel;
import oharastream.ohara.stream.OStream;
import oharastream.ohara.stream.Stream;
import oharastream.ohara.stream.config.StreamDefUtils;
import oharastream.ohara.stream.config.StreamSetting;
import oharastream.ohara.stream.metric.MetricFactory;
import oharastream.ohara.testing.WithBroker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// TODO: the stream requires many arguments from env variables.
// This tests do not care the rules required by stream.
// Fortunately (or unfortunately), stream lacks of enough checks to variables so the
// non-completed settings to stream works well in this test ... by chia
public class TestSimpleStreamCounter extends WithBroker {

  private static final Duration timeout = Duration.ofSeconds(10);
  private static final TopicKey fromKey = TopicKey.of(CommonUtils.randomString(), "metric-from");
  private static final TopicKey toKey = TopicKey.of(CommonUtils.randomString(), "metric-to");

  private final TopicAdmin client = TopicAdmin.of(testUtil().brokersConnProps());
  private final Producer<Row, byte[]> producer =
      Producer.builder()
          .connectionProps(client.connectionProps())
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build();
  private final Consumer<Row, byte[]> consumer =
      Consumer.builder()
          .topicKey(toKey)
          .connectionProps(client.connectionProps())
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build();

  @BeforeEach
  public void setup() {
    int partitions = 1;
    short replications = 1;
    client
        .topicCreator()
        .numberOfPartitions(partitions)
        .numberOfReplications(replications)
        .topicKey(fromKey)
        .create();

    try {
      producer
          .sender()
          .key(Row.of(Cell.of("bar", "foo")))
          .value(new byte[0])
          .topicKey(fromKey)
          .send()
          .get();
      producer
          .sender()
          .key(Row.of(Cell.of("hello", "world")))
          .value(new byte[0])
          .topicKey(fromKey)
          .send()
          .get();
    } catch (Exception e) {
      Assertions.fail();
    }
  }

  @Test
  public void testMetrics() {
    DirectWriteStream app = new DirectWriteStream();
    Stream.execute(
        app.getClass(),
        Map.of(
            StreamDefUtils.GROUP_DEFINITION.key(), CommonUtils.randomString(5),
            StreamDefUtils.NAME_DEFINITION.key(), "TestSimpleStreamCounter",
            StreamDefUtils.BROKER_DEFINITION.key(), client.connectionProps(),
            StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key(),
                TopicKey.toJsonString(List.of(fromKey)),
            StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key(), TopicKey.toJsonString(List.of(toKey))));

    // wait until topic has data
    CommonUtils.await(() -> consumer.poll(timeout).size() > 0, Duration.ofSeconds(30));

    // there should be two counter bean (in_topic, to_topic)
    Assertions.assertEquals(2, BeanChannel.local().counterMBeans().size());

    BeanChannel.local()
        .counterMBeans()
        .forEach(
            bean -> {
              if (bean.item().equals(MetricFactory.IOType.TOPIC_IN.name()))
                // input counter bean should have exactly two record size
                Assertions.assertEquals(2, Math.toIntExact(bean.getValue()));
              else
                // output counter bean should have exactly one record size (after filter)
                Assertions.assertEquals(1, Math.toIntExact(bean.getValue()));
            });
  }

  public static class DirectWriteStream extends Stream {

    @Override
    public void start(OStream<Row> ostream, StreamSetting streamSetting) {

      ostream.filter(row -> row.names().contains("bar")).map(row -> row).start();
    }
  }
}
