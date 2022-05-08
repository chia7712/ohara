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

package oharastream.ohara.kafka.connector.csv;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.kafka.connector.RowSinkRecord;
import oharastream.ohara.kafka.connector.RowSinkTask;
import oharastream.ohara.kafka.connector.TaskSetting;
import oharastream.ohara.kafka.connector.TopicOffset;
import oharastream.ohara.kafka.connector.TopicPartition;
import oharastream.ohara.kafka.connector.csv.sink.CsvDataWriter;
import oharastream.ohara.kafka.connector.csv.sink.CsvSinkConfig;
import oharastream.ohara.kafka.connector.csv.sink.DataWriter;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CsvSinkTask ia a Ohara RowSinkTask wrapper. It used to convert the RowSinkRecord to CSV files.
 * Ohara developers should extend this class rather than RowSinkTask in order to let the conversion
 * from RowSinkRecord to CSV files work easily.
 */
public abstract class CsvSinkTask extends RowSinkTask {
  private static final Logger log = LoggerFactory.getLogger(CsvSinkTask.class);
  private DataWriter writer;

  /**
   * Returns the FileSystem implementation for this Task.
   *
   * @param setting initial settings
   * @return a FileSystem instance
   */
  public abstract FileSystem fileSystem(TaskSetting setting);

  @Override
  protected void run(TaskSetting setting) {
    writer =
        new CsvDataWriter(
            CsvSinkConfig.of(setting, setting.columns()), rowContext, fileSystem(setting));
  }

  @Override
  protected void openPartitions(List<TopicPartition> partitions) {
    writer.attach(partitions);
  }

  @Override
  protected void putRecords(List<RowSinkRecord> records) {
    writer.write(records);
  }

  @Override
  public Map<TopicPartition, TopicOffset> preCommitOffsets(
      Map<TopicPartition, TopicOffset> offsets) {
    Map<TopicPartition, TopicOffset> offsetsToCommit = new HashMap<>();

    for (Map.Entry<TopicPartition, Long> entry : writer.getCommittedOffsetsAndReset().entrySet()) {
      log.debug(
          "Found last committed offset {} for topic partition {}",
          entry.getValue(),
          entry.getKey());
      offsetsToCommit.put(entry.getKey(), new TopicOffset(null, entry.getValue()));
    }

    log.debug("Returning committed offsets {}", offsetsToCommit);
    return offsetsToCommit;
  }

  @Override
  protected void closePartitions(List<TopicPartition> partitions) {
    if (writer != null) {
      writer.detach(partitions);
    }
  }

  @Override
  protected void terminate() {
    Releasable.close(writer);
  }
}
