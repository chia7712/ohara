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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ClassType;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.setting.WithDefinitions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRowPartitioner extends OharaTest {

  @Test
  public void nonRowShouldNotBePassedToSubClass() {
    CountRowPartitioner custom = new CountRowPartitioner();
    Node node = new Node(0, "localhost", 99);
    Node[] nodes = new Node[] {node};
    TopicKey key = TopicKey.of("a", "b");
    PartitionInfo partitionInfo = new PartitionInfo(key.topicNameOnKafka(), 1, node, nodes, nodes);
    org.apache.kafka.common.Cluster cluster =
        new org.apache.kafka.common.Cluster(
            "aa", Arrays.asList(nodes), List.of(partitionInfo), Set.of(), Set.of());
    custom.partition(key.topicNameOnKafka(), "sss", "sss".getBytes(), null, null, cluster);
    Assertions.assertEquals(0, custom.count.get());

    Row row = Row.of(Cell.of("a", "b"));
    custom.partition(key.topicNameOnKafka(), row, Serializer.ROW.to(row), null, null, cluster);
    Assertions.assertEquals(1, custom.count.get());
  }

  @Test
  public void testKind() {
    Assertions.assertEquals(
        ClassType.PARTITIONER.key(),
        new RowDefaultPartitioner()
            .settingDefinitions()
            .get(WithDefinitions.KIND_KEY)
            .defaultString());
    Assertions.assertEquals(
        ClassType.PARTITIONER.key(),
        new CountRowPartitioner()
            .settingDefinitions()
            .get(WithDefinitions.KIND_KEY)
            .defaultString());
  }
}
