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

import java.util.Objects;
import oharastream.ohara.common.util.CommonUtils;
import org.apache.kafka.common.Node;

public final class PartitionNode {

  public static PartitionNode of(Node node) {
    return new PartitionNode(node.id(), node.host(), node.port());
  }

  private final int id;
  private final String host;
  private final int port;

  public PartitionNode(int id, String host, int port) {
    this.id = id;
    this.host = CommonUtils.requireNonEmpty(host);
    this.port = port;
  }

  public int id() {
    return id;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  @Override
  public int hashCode() {
    int result = 31 + ((host == null) ? 0 : host.hashCode());
    result = 31 * result + id;
    result = 31 * result + port;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof PartitionNode) {
      PartitionNode other = (PartitionNode) obj;
      return id == other.id && port == other.port && Objects.equals(host, other.host);
    }
    return false;
  }

  @Override
  public String toString() {
    return host + ":" + port + " (id: " + id + ")";
  }
}
