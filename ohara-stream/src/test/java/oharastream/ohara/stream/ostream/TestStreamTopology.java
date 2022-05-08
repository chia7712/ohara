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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.stream.OStream;
import oharastream.ohara.stream.Stream;
import oharastream.ohara.stream.config.StreamDefUtils;
import oharastream.ohara.stream.config.StreamSetting;
import oharastream.ohara.stream.data.Poneglyph;
import oharastream.ohara.stream.data.Stele;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStreamTopology extends OharaTest {
  private static TopicKey fromKey =
      TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString());
  private static TopicKey toKey =
      TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString());
  private static final String join = "join_topic";

  @Test
  public void testGetTopology() {
    DescribeStream app = new DescribeStream();
    Stream.execute(
        app.getClass(),
        Map.of(
            StreamDefUtils.GROUP_DEFINITION.key(), CommonUtils.randomString(5),
            StreamDefUtils.NAME_DEFINITION.key(), "TestStreamTopology",
            StreamDefUtils.BROKER_DEFINITION.key(), "fake",
            StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key(),
                TopicKey.toJsonString(List.of(fromKey)),
            StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key(), TopicKey.toJsonString(List.of(toKey))));
  }

  public static class DescribeStream extends Stream {

    @Override
    public void start(OStream<Row> ostream, StreamSetting streamSetting) {
      List<Poneglyph> poneglyph =
          ostream
              .filter(row -> !row.cell(0).value().toString().isEmpty())
              .map(row -> Row.of(row.cell(0)))
              .leftJoin(
                  join, Conditions.create().add(List.of(Map.entry("pk", "fk"))), (r1, r2) -> r1)
              .groupByKey(List.of("key"))
              .count()
              .getPoneglyph();

      // It should have four "steles", i.e., four process topology
      Assertions.assertEquals(4, poneglyph.size());

      // Topics should be contained in topologies
      Arrays.asList(fromKey.topicNameOnKafka(), toKey.topicNameOnKafka(), join)
          .forEach(
              topic ->
                  Assertions.assertTrue(
                      poneglyph.stream()
                          .flatMap(p -> p.getSteles().stream())
                          .filter(s -> s.getKind().equals("Source") || s.getKind().equals("Sink"))
                          .map(Stele::getName)
                          .anyMatch(topologyTopic -> topologyTopic.contains(topic))));
    }
  }
}
