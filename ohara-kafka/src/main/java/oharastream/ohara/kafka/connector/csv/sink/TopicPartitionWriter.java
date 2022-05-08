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
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicPartitionWriter implements Releasable {
  private static final Logger LOG = LoggerFactory.getLogger(TopicPartitionWriter.class);

  private final Time time;
  private final TopicPartition tp;
  private final CsvRecordWriterProvider format;
  private final RowSinkContext context;
  private final CsvSinkConfig conf;

  private final int flushSize;
  private final long rotateIntervalMs;
  private final String topicsDir;
  private final Queue<RowSinkRecord> buffer;
  private final Map<String, CsvRecordWriter> writers;
  private final Map<String, Long> startOffsets;
  private final Map<String, String> commitFiles;

  private long currentOffset;
  private Long offsetToCommit;
  private long nextScheduledRotation;
  private int recordCount;

  private static final Time SYSTEM_TIME = new SystemTime();

  public TopicPartitionWriter(
      TopicPartition tp,
      CsvRecordWriterProvider format,
      CsvSinkConfig conf,
      RowSinkContext context) {
    this.time = SYSTEM_TIME;

    this.tp = tp;
    this.format = format;
    this.context = context;
    this.conf = conf;

    this.flushSize = conf.flushSize();
    this.rotateIntervalMs = conf.rotateIntervalMs();
    this.topicsDir = conf.outputFolder();

    this.buffer = new LinkedList<>();
    this.writers = new HashMap<>();
    this.startOffsets = new HashMap<>();
    this.commitFiles = new HashMap<>();
    this.currentOffset = -1L;

    LOG.trace(
        "Configuration: flushSize={}, rotateIntervalMs={}, topicsDir={}",
        flushSize,
        rotateIntervalMs,
        topicsDir);

    // Initialize scheduled rotation timer if applicable
    setNextScheduledRotation();
  }

  public void buffer(RowSinkRecord sinkRecord) {
    buffer.add(sinkRecord);
  }

  public void write() {
    long now = time.milliseconds();
    pause();

    while (!buffer.isEmpty()) {
      rotateOrWrite(now);
    }

    commitOnTimeIfNoData(now);
    resume();
  }

  private void pause() {
    LOG.trace("Pausing writer for topic-partition '{}'", tp);
    // TODO: context.pause(tp);
  }

  private void resume() {
    LOG.trace("Resuming writer for topic-partition '{}'", tp);
    // TODO: context.resume(tp);
  }

  private void rotateOrWrite(long now) {
    if (rotateOnTime(now)) {
      commitFiles();
    } else {
      writeRecord(buffer.poll());

      if (rotateOnSize()) {
        LOG.info(
            "Starting commit and rotation for topic partition {} with start offset {}",
            tp,
            startOffsets);
        commitFiles();
      }
    }
  }

  private void writeRecord(RowSinkRecord record) {
    String encodedPartition = encodePartition(record);
    currentOffset = record.offset();

    if (!startOffsets.containsKey(encodedPartition)) {
      LOG.trace("Setting writer's start offset for '{}' to {}", encodedPartition, currentOffset);
      startOffsets.put(encodedPartition, currentOffset);
    }

    getWriter(encodedPartition).write(record);
    recordCount++;
  }

  private String encodePartition(RowSinkRecord sinkRecord) {
    return "partition" + sinkRecord.partition();
  }

  private CsvRecordWriter getWriter(String encodedPartition) {
    if (writers.containsKey(encodedPartition)) {
      return writers.get(encodedPartition);
    }
    String commitFile = getCommitFile(encodedPartition);
    CsvRecordWriter writer = format.getRecordWriter(conf, commitFile);
    writers.put(encodedPartition, writer);
    return writer;
  }

  private String getCommitFile(String encodedPartition) {
    String commitFile;
    if (commitFiles.containsKey(encodedPartition)) {
      commitFile = commitFiles.get(encodedPartition);
    } else {
      long startOffset = startOffsets.get(encodedPartition);
      String directory = getDirectory(encodedPartition);
      commitFile =
          FileUtils.committedFileName(topicsDir, directory, tp, startOffset, format.getExtension());
      commitFiles.put(encodedPartition, commitFile);
    }

    return commitFile;
  }

  private String getDirectory(String encodedPartition) {
    return FileUtils.generatePartitionedPath(tp.topicKey(), encodedPartition);
  }

  private boolean rotateOnTime(long now) {
    if (recordCount <= 0) {
      return false;
    }

    LOG.trace("Checking rotation on time with recordCount '{}'", recordCount);

    boolean scheduledRotation = rotateIntervalMs > 0 && now >= nextScheduledRotation;
    LOG.debug(
        "Should apply scheduled rotation: (rotateIntervalMs: '{}', nextScheduledRotation:"
            + " '{}', now: '{}')? {}",
        rotateIntervalMs,
        nextScheduledRotation,
        now,
        scheduledRotation);

    if (scheduledRotation) {
      setNextScheduledRotation();
    }
    return scheduledRotation;
  }

  private boolean rotateOnSize() {
    boolean messageSizeRotation = recordCount >= flushSize;
    LOG.trace(
        "Should apply size-based rotation (count {} >= flush size {})? {}",
        recordCount,
        flushSize,
        messageSizeRotation);
    return messageSizeRotation;
  }

  private void commitOnTimeIfNoData(long now) {
    if (buffer.isEmpty()) {
      // committing files after waiting for rotateIntervalMs time but less than flush.size
      // records available
      if (recordCount > 0 && rotateOnTime(now)) {
        LOG.info(
            "Committing files after waiting for rotateIntervalMs time but less than flush.size records available.");
        commitFiles();
      }
    }
  }

  private void commitFiles() {
    for (Map.Entry<String, String> entry : commitFiles.entrySet()) {
      commitFile(entry.getKey());
      LOG.debug("Committed {} for {}", entry.getValue(), tp);
    }
    offsetToCommit = currentOffset + 1;
    commitFiles.clear();
    recordCount = 0;
    LOG.info(
        "Files committed to FileSystem. Target commit offset for {} is {}", tp, offsetToCommit);
  }

  private void commitFile(String encodedPartition) {
    if (writers.containsKey(encodedPartition)) {
      // Commits the file and closes the underlying output stream.
      writers.get(encodedPartition).commit();
      writers.remove(encodedPartition);
      LOG.debug("Removed writer for '{}'", encodedPartition);
    }
    startOffsets.remove(encodedPartition);
  }

  private void setNextScheduledRotation() {
    if (rotateIntervalMs > 0) {
      long now = time.milliseconds();
      nextScheduledRotation = now + rotateIntervalMs;
    }
  }

  public Long getOffsetToCommitAndReset() {
    Long latest = offsetToCommit;
    offsetToCommit = null;
    return latest;
  }

  public void close() {
    LOG.debug("Closing TopicPartitionWriter {}", tp);
    for (CsvRecordWriter writer : writers.values()) {
      Releasable.close(writer);
    }
    writers.clear();
    startOffsets.clear();
  }

  @VisibleForTesting
  public long getRecordCount() {
    return recordCount;
  }

  @VisibleForTesting
  public Long getCommittedOffset() {
    return offsetToCommit;
  }
}
