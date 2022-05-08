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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import oharastream.ohara.common.setting.TopicKey;

/**
 * get TopicDescription from kafka client
 *
 * @see TopicAdmin ;
 */
public class TopicDescription {
  private final TopicKey topicKey;
  private final List<PartitionInfo> partitionInfos;
  private final List<TopicOption> options;

  public TopicDescription(
      TopicKey topicKey, List<PartitionInfo> partitionInfos, List<TopicOption> options) {
    this.topicKey = Objects.requireNonNull(topicKey);
    this.partitionInfos = Collections.unmodifiableList(partitionInfos);
    this.options = Collections.unmodifiableList(options);
  }

  public TopicKey topicKey() {
    return topicKey;
  }

  public List<PartitionInfo> partitionInfos() {
    return partitionInfos;
  }

  public int numberOfPartitions() {
    return partitionInfos.size();
  }

  public short numberOfReplications() {
    return (short) (partitionInfos.isEmpty() ? 0 : partitionInfos.get(0).replicas().size());
  }

  public List<TopicOption> options() {
    return options;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicDescription that = (TopicDescription) o;
    return partitionInfos.equals(that.partitionInfos)
        && Objects.equals(topicKey, that.topicKey)
        && Objects.equals(options, that.options);
  }

  @Override
  public String toString() {
    return "topicKey="
        + topicKey
        + ", partitionInfos="
        + partitionInfos.stream().map(PartitionInfo::toString).collect(Collectors.joining(","))
        + ", options="
        + options;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicKey, partitionInfos, options);
  }
}
