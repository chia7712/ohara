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

import java.util.*;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.kafka.TimestampType;
import oharastream.ohara.kafka.connector.RowSinkContext;
import oharastream.ohara.kafka.connector.RowSinkRecord;
import oharastream.ohara.kafka.connector.TaskSetting;
import oharastream.ohara.kafka.connector.TopicPartition;
import oharastream.ohara.kafka.connector.csv.sink.CsvSinkConfig;

public abstract class CsvSinkTestBase extends OharaTest {
  protected static final TopicKey TOPIC = TopicKey.of("test", "topic");
  protected static final int PARTITION = 12;
  protected static final int PARTITION2 = 13;
  protected static final int PARTITION3 = 14;
  protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
  protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
  protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);

  protected CsvSinkConfig config;
  protected Map<String, String> props;
  protected RowSinkContext context;

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(CsvConnectorDefinitions.FLUSH_SIZE_KEY, "3");
    return props;
  }

  protected void setUp() {
    props = createProps();
    config = CsvSinkConfig.of(TaskSetting.of(props));
    Set<TopicPartition> assignment = new HashSet<>();
    assignment.add(TOPIC_PARTITION);
    assignment.add(TOPIC_PARTITION2);
    context = new CsvSinkTestBase.MockSinkContext(assignment);
  }

  protected Row createRow(String key) {
    return Row.of(
        Cell.of("key", key),
        Cell.of("boolean", true),
        Cell.of("int", 12),
        Cell.of("long", 12L),
        Cell.of("float", 12.2F),
        Cell.of("double", 12.2D));
  }

  protected RowSinkRecord createRecord(Row row, long offset) {
    return createRecord(TOPIC, PARTITION, row, offset);
  }

  protected RowSinkRecord createRecord(TopicKey topicKey, int partition, Row row, long offset) {
    return RowSinkRecord.builder()
        .topicKey(topicKey)
        .partition(partition)
        .row(row)
        .offset(offset)
        .timestamp(0)
        .timestampType(TimestampType.CREATE_TIME)
        .build();
  }

  protected List<RowSinkRecord> createRecords(int size) {
    return createRecords(size, 0);
  }

  protected List<RowSinkRecord> createRecords(int size, long startOffset) {
    return createRecords(size, startOffset, Set.of(new TopicPartition(TOPIC, PARTITION)));
  }

  protected List<RowSinkRecord> createRecords(
      int size, long startOffset, Set<TopicPartition> partitions) {
    List<RowSinkRecord> records = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      for (long offset = startOffset; offset < startOffset + size; ++offset) {
        records.add(createRecord(tp.topicKey(), tp.partition(), createRow("#" + offset), offset));
      }
    }
    return records;
  }

  protected static class MockSinkContext implements RowSinkContext {
    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private final Set<TopicPartition> assignment;

    public MockSinkContext(Set<TopicPartition> assignment) {
      this.assignment = assignment;
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
      this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
      offsets.put(tp, offset);
    }

    @Override
    public Set<TopicPartition> assignment() {
      return assignment;
    }
  }
}
