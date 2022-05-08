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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.kafka.connector.RowSinkRecord;
import oharastream.ohara.kafka.connector.TopicPartition;
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions;
import oharastream.ohara.kafka.connector.csv.WithMockStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCsvDataWriter extends WithMockStorage {
  private final Map<String, String> localProps = new HashMap<>();
  private final File topicsDir = createTemporaryFolder();

  private CsvDataWriter dataWriter;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(CsvConnectorDefinitions.OUTPUT_FOLDER_KEY, topicsDir.getPath());
    props.put(CsvConnectorDefinitions.FILE_NEED_HEADER_KEY, "false");
    props.putAll(localProps);
    return props;
  }

  @Override
  protected void setUp() {
    super.setUp();
    dataWriter = new CsvDataWriter(config, context, fs);
  }

  @Test
  public void testWriteRecord() {
    localProps.put(CsvConnectorDefinitions.FLUSH_SIZE_KEY, "3");

    setUp();
    List<RowSinkRecord> sinkRecords = createRecords(7);

    dataWriter.write(sinkRecords);
    Map<TopicPartition, Long> committedOffsets = dataWriter.getCommittedOffsetsAndReset();
    Assertions.assertNotNull(committedOffsets.get(TOPIC_PARTITION));
    Assertions.assertEquals(6, committedOffsets.get(TOPIC_PARTITION).intValue());
    dataWriter.close();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsSpanningMultipleParts() {
    localProps.put(CsvConnectorDefinitions.FLUSH_SIZE_KEY, "10000");
    setUp();
    List<RowSinkRecord> sinkRecords = createRecords(11000);

    dataWriter.write(sinkRecords);
    dataWriter.close();
    dataWriter.close();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testCommitOnSizeRotation() {
    localProps.put(CsvConnectorDefinitions.FLUSH_SIZE_KEY, "3");
    setUp();

    List<RowSinkRecord> sinkRecords;
    Map<TopicPartition, Long> offsetsToCommit;

    sinkRecords = createRecords(3, 0);
    dataWriter.write(sinkRecords);
    offsetsToCommit = dataWriter.getCommittedOffsetsAndReset();
    verifyOffset(offsetsToCommit, 3);

    sinkRecords = createRecords(2, 3);
    dataWriter.write(sinkRecords);
    offsetsToCommit = dataWriter.getCommittedOffsetsAndReset();
    // Actual values are null, we set to negative for the verifier.
    verifyOffset(offsetsToCommit, -1);

    sinkRecords = createRecords(1, 5);
    dataWriter.write(sinkRecords);
    offsetsToCommit = dataWriter.getCommittedOffsetsAndReset();
    verifyOffset(offsetsToCommit, 6);

    sinkRecords = createRecords(3, 6);
    dataWriter.write(sinkRecords);
    offsetsToCommit = dataWriter.getCommittedOffsetsAndReset();
    verifyOffset(offsetsToCommit, 9);

    dataWriter.close();
  }

  @Test
  public void testAssignment() {
    setUp();

    Assertions.assertEquals(2, dataWriter.getAssignment().size());
    Assertions.assertEquals(2, dataWriter.getTopicPartitionWriters().size());

    dataWriter.attach(Set.of(TOPIC_PARTITION3));
    Assertions.assertEquals(3, dataWriter.getAssignment().size());
    Assertions.assertEquals(3, dataWriter.getTopicPartitionWriters().size());

    dataWriter.write(List.of());

    dataWriter.close();
    Assertions.assertEquals(0, dataWriter.getAssignment().size());
    Assertions.assertEquals(0, dataWriter.getTopicPartitionWriters().size());
  }

  protected void verify(List<RowSinkRecord> sinkRecords, long[] validOffsets) {
    verify(sinkRecords, validOffsets, Set.of(TOPIC_PARTITION));
  }

  protected void verify(
      List<RowSinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long size = validOffsets[i] - startOffset;

        String extension = ".csv";
        String filePath =
            FileUtils.committedFileName(
                config.outputFolder(), getDirectory(tp), tp, startOffset, extension);
        Collection<String> data = readData(filePath);

        Assertions.assertEquals(size, data.size());
        verifyContents(sinkRecords, j, data);
        j += size;
      }
    }
  }

  protected void verifyContents(
      List<RowSinkRecord> expectedRecords, int startIndex, Collection<String> data) {
    for (Object line : data) {
      RowSinkRecord expectedRecord = expectedRecords.get(startIndex++);
      List<Column> expectedSchema = RecordUtils.newSchema(null, expectedRecord);
      String expectedLine = RecordUtils.toLine(expectedSchema, expectedRecord);
      Assertions.assertEquals(expectedLine, line);
    }
  }

  protected void verifyOffset(Map<TopicPartition, Long> actualOffsets, long validOffset) {
    if (validOffset > 0) {
      Assertions.assertNotNull(actualOffsets.get(TOPIC_PARTITION));
      Assertions.assertEquals(validOffset, actualOffsets.get(TOPIC_PARTITION).intValue());
    } else {
      Assertions.assertNull(actualOffsets.get(TOPIC_PARTITION));
    }
  }

  private String getDirectory(TopicPartition tp) {
    String encodedPartition = "partition" + tp.partition();
    return FileUtils.generatePartitionedPath(tp.topicKey(), encodedPartition);
  }
}
