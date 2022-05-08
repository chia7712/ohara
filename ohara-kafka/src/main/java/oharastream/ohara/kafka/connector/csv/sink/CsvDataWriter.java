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

import java.util.*;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.kafka.connector.RowSinkContext;
import oharastream.ohara.kafka.connector.RowSinkRecord;
import oharastream.ohara.kafka.connector.TopicPartition;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CsvDataWriter.class);

  private final Set<TopicPartition> assignment;
  private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
  private final RowSinkContext context;
  private final CsvSinkConfig config;
  private final FileSystem fileSystem;
  private final CsvRecordWriterProvider writerProvider;

  public CsvDataWriter(CsvSinkConfig config, RowSinkContext context, FileSystem fileSystem) {
    assignment = new HashSet<>();
    topicPartitionWriters = new HashMap<>();
    this.context = context;
    this.config = config;
    this.fileSystem = fileSystem;
    this.writerProvider = new CsvRecordWriterProvider(fileSystem);
    attach(context.assignment());
  }

  @Override
  public void attach(Collection<TopicPartition> partitions) {
    assignment.addAll(partitions);
    assignment.forEach(
        tp -> {
          if (!topicPartitionWriters.containsKey(tp)) {
            topicPartitionWriters.put(
                tp, new TopicPartitionWriter(tp, writerProvider, config, context));
          }
        });
  }

  @Override
  public void write(Collection<RowSinkRecord> records) {
    records.forEach(record -> topicPartitionWriters.get(record.topicPartition()).buffer(record));
    assignment.forEach(tp -> topicPartitionWriters.get(tp).write());
  }

  @Override
  public void detach(Collection<TopicPartition> partitions) {
    partitions.forEach(
        tp -> {
          Releasable.close(topicPartitionWriters.get(tp));
          topicPartitionWriters.remove(tp);
        });
  }

  @Override
  public void close() {
    detach(assignment);
    assignment.clear();
    topicPartitionWriters.clear();
    Releasable.close(fileSystem);
  }

  @Override
  public Map<TopicPartition, Long> getCommittedOffsetsAndReset() {
    Map<TopicPartition, Long> offsetsToCommit = new HashMap<>();
    for (TopicPartition tp : assignment) {
      Long offset = topicPartitionWriters.get(tp).getOffsetToCommitAndReset();
      if (offset != null) {
        LOG.trace("Forwarding to framework request to commit offset: {} for {}", offset, tp);
        offsetsToCommit.put(tp, offset);
      }
    }
    return offsetsToCommit;
  }

  @VisibleForTesting
  public Map<TopicPartition, TopicPartitionWriter> getTopicPartitionWriters() {
    return topicPartitionWriters;
  }

  @VisibleForTesting
  public Set<TopicPartition> getAssignment() {
    return assignment;
  }
}
