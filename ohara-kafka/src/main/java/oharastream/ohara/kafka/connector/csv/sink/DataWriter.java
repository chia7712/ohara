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

package oharastream.ohara.kafka.connector.csv.sink;

import java.util.Collection;
import java.util.Map;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.kafka.connector.RowSinkRecord;
import oharastream.ohara.kafka.connector.TopicPartition;

/**
 * DataWriter is a Ohara RowSinkTask helper that takes records from Ohara and sends them to another
 * system. The DataWriter instance is assigned a set of partitions by the RowSinkTask and will
 * handle all records received from those partitions.
 */
public interface DataWriter extends Releasable {
  /**
   * The DataWriter use the method to create TopicPartitionWriters for newly assigned partitions in
   * case of partition rebalancing.
   *
   * @param partitions The list of partitions that are now assigned to the DataWriter
   */
  void attach(Collection<TopicPartition> partitions);

  /**
   * Write the records in the DataWriter.
   *
   * @param records the set of records to send
   */
  void write(Collection<RowSinkRecord> records);

  /**
   * The DataWriter use the method to close TopicPartitionWriters for partitions that are no longer
   * assigned to the DataWriter.
   *
   * @param partitions The list of partitions
   */
  void detach(Collection<TopicPartition> partitions);

  /** Perform any cleanup to close this writer. */
  void close();

  /** @return a map of offsets by topic-partition that are safe to commit. */
  Map<TopicPartition, Long> getCommittedOffsetsAndReset();
}
