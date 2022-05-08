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

package oharastream.ohara.common.setting;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** the type of official classes which implement the definitions. */
public enum ClassType {
  SOURCE(
      new HashSet<>(
          Arrays.asList(
              "oharastream.ohara.kafka.connector.RowSourceConnector",
              "oharastream.ohara.shabondi.ShabondiSource"))),
  SINK(
      new HashSet<>(
          Arrays.asList(
              "oharastream.ohara.kafka.connector.RowSinkConnector",
              "oharastream.ohara.shabondi.ShabondiSink"))),
  PARTITIONER(Set.of("oharastream.ohara.kafka.RowPartitioner")),
  STREAM(Set.of("oharastream.ohara.stream.Stream")),
  TOPIC(Set.of()),
  UNKNOWN(Set.of());

  final Set<String> bases;

  ClassType(Set<String> bases) {
    this.bases = bases;
  }

  /**
   * the unique key of this type
   *
   * @return unique key
   */
  public String key() {
    return name().toLowerCase();
  }

  /**
   * get the type of class name
   *
   * @param className class name
   * @return type
   */
  public static ClassType classOf(String className) {
    return Arrays.stream(ClassType.values())
        .filter(t -> t.bases.contains(className))
        .findFirst()
        .orElse(ClassType.UNKNOWN);
  }
}
