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
import java.util.stream.Collectors;
import oharastream.ohara.common.setting.TopicKey;

public final class Cluster {

  private static List<PartitionInfo> partitionInfos(
      List<org.apache.kafka.common.PartitionInfo> partitionInfos) {
    return partitionInfos.stream().map(PartitionInfo::of).collect(Collectors.toUnmodifiableList());
  }

  public static Cluster of(org.apache.kafka.common.Cluster cluster) {
    return new Cluster(
        cluster.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        PartitionNode.of(node),
                        partitionInfos(cluster.partitionsForNode(node.id()))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)),
        cluster.topics().stream()
            .map(topic -> Map.entry(topic, partitionInfos(cluster.partitionsForTopic(topic))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  private final Map<PartitionNode, List<PartitionInfo>> partitionsByNode;
  private final Map<String, List<PartitionInfo>> partitionsByTopic;

  private Cluster(
      Map<PartitionNode, List<PartitionInfo>> partitionsByNode,
      Map<String, List<PartitionInfo>> partitionsByTopic) {
    this.partitionsByNode = Objects.requireNonNull(partitionsByNode);
    this.partitionsByTopic = Objects.requireNonNull(partitionsByTopic);
  }

  public Map<PartitionNode, List<PartitionInfo>> partitionsByNode() {
    return partitionsByNode;
  }

  public Map<String, List<PartitionInfo>> partitionsByTopic() {
    return partitionsByTopic;
  }

  public List<PartitionInfo> partitionInfos(TopicKey key) {
    return partitionsByTopic.getOrDefault(key.topicNameOnKafka(), List.of());
  }
}
