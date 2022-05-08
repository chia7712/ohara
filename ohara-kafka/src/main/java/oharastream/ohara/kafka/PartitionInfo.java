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
import java.util.stream.Stream;

public final class PartitionInfo {

  /**
   * @param partitionInfo kafka partitionInfo
   * @return ohara partition info. Noted, both offsets are -1 since kafka partition doesn't carry
   *     such information.
   */
  public static PartitionInfo of(org.apache.kafka.common.PartitionInfo partitionInfo) {
    return new PartitionInfo(
        partitionInfo.partition(),
        PartitionNode.of(partitionInfo.leader()),
        Stream.of(partitionInfo.replicas())
            .map(PartitionNode::of)
            .collect(Collectors.toUnmodifiableList()),
        Stream.of(partitionInfo.inSyncReplicas())
            .map(PartitionNode::of)
            .collect(Collectors.toUnmodifiableList()),
        -1,
        -1);
  }

  private final int id;
  private final PartitionNode leader;
  private final List<PartitionNode> replicas;
  private final List<PartitionNode> inSyncReplicas;
  private final long beginningOffset;
  private final long endOffset;

  public PartitionInfo(
      int id,
      PartitionNode leader,
      List<PartitionNode> replicas,
      List<PartitionNode> inSyncReplicas,
      long beginningOffset,
      long endOffset) {
    this.id = id;
    this.leader = leader;
    this.replicas = Collections.unmodifiableList(replicas);
    this.inSyncReplicas = Collections.unmodifiableList(inSyncReplicas);
    this.beginningOffset = beginningOffset;
    this.endOffset = endOffset;
  }

  public int id() {
    return id;
  }

  public PartitionNode leader() {
    return leader;
  }

  public List<PartitionNode> replicas() {
    return replicas;
  }

  public List<PartitionNode> inSyncReplicas() {
    return inSyncReplicas;
  }

  public long beginningOffset() {
    return beginningOffset;
  }

  public long endOffset() {
    return endOffset;
  }

  public String toString() {
    return "(partition="
        + id
        + ", leader="
        + leader
        + ", replicas="
        + replicas.stream().map(PartitionNode::toString).collect(Collectors.joining(","))
        + ", inSyncReplicas="
        + inSyncReplicas.stream().map(PartitionNode::toString).collect(Collectors.joining(","))
        + ", beginningOffset="
        + beginningOffset
        + ", endOffset="
        + endOffset
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof PartitionInfo) {
      PartitionInfo that = (PartitionInfo) obj;
      return id == that.id
          && Objects.equals(leader, that.leader)
          && Objects.equals(replicas, that.replicas)
          && Objects.equals(inSyncReplicas, that.inSyncReplicas)
          && Objects.equals(beginningOffset, that.beginningOffset)
          && Objects.equals(endOffset, that.endOffset);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + (leader != null ? leader.hashCode() : 0);
    result = 31 * result + (replicas != null ? replicas.hashCode() : 0);
    result = 31 * result + (inSyncReplicas != null ? inSyncReplicas.hashCode() : 0);
    result = 31 * result + Long.hashCode(beginningOffset);
    result = 31 * result + Long.hashCode(endOffset);
    return result;
  }
}
