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

package oharastream.ohara.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.Producer;
import oharastream.ohara.kafka.TopicAdmin;
import oharastream.ohara.stream.config.StreamDefUtils;
import oharastream.ohara.stream.examples.WordCountExample;
import oharastream.ohara.testing.WithBroker;
import org.junit.jupiter.api.Test;

public class TestWordCountExample extends WithBroker {

  @Test
  public void testCase() {
    final TopicAdmin client = TopicAdmin.of(testUtil().brokersConnProps());
    final Producer<Row, byte[]> producer =
        Producer.builder()
            .connectionProps(client.connectionProps())
            .keySerializer(Serializer.ROW)
            .valueSerializer(Serializer.BYTES)
            .build();
    final int partitions = 1;
    final short replications = 1;
    TopicKey fromTopic = TopicKey.of("default", "text-input");
    TopicKey toTopic = TopicKey.of("default", "count-output");

    // prepare ohara environment
    Map<String, String> settings = new HashMap<>();
    settings.putIfAbsent(StreamDefUtils.BROKER_DEFINITION.key(), client.connectionProps());
    settings.putIfAbsent(StreamDefUtils.GROUP_DEFINITION.key(), CommonUtils.randomString(5));
    settings.putIfAbsent(StreamDefUtils.NAME_DEFINITION.key(), "TestWordCountExample");
    settings.putIfAbsent(
        StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key(),
        TopicKey.toJsonString(java.util.List.of(fromTopic)));
    settings.putIfAbsent(
        StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key(),
        TopicKey.toJsonString(java.util.List.of(toTopic)));
    StreamTestUtils.createTopic(client, fromTopic, partitions, replications);
    StreamTestUtils.createTopic(client, toTopic, partitions, replications);
    // prepare data
    List<Row> rows =
        java.util.stream.Stream.of("hello", "ohara", "stream", "world", "of", "stream")
            .map(str -> Row.of(Cell.of("word", str)))
            .collect(Collectors.toUnmodifiableList());
    StreamTestUtils.produceData(producer, rows, fromTopic);

    // run example
    WordCountExample app = new WordCountExample();
    Stream.execute(app.getClass(), settings);

    // Assert the result
    List<Row> expected =
        java.util.stream.Stream.of(
                Row.of(Cell.of("word", "stream"), Cell.of("count", 2L)),
                Row.of(Cell.of("word", "world"), Cell.of("count", 1L)))
            .collect(Collectors.toUnmodifiableList());
    // Since the result of "count" is "accumulate", we will get the same size as input count
    StreamTestUtils.assertResult(client, toTopic, expected, rows.size());
  }
}
