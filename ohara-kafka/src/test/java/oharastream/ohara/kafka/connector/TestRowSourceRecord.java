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

package oharastream.ohara.kafka.connector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ConnectorKey;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.ByteUtils;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.json.ConnectorFormatter;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRowSourceRecord extends OharaTest {

  @Test
  public void requireTopic() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSourceRecord.builder()
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .build());
  }

  @Test
  public void requireRow() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> RowSourceRecord.builder().topicKey(TopicKey.of("g", "n")).build());
  }

  @Test
  public void nullRow() {
    Assertions.assertThrows(NullPointerException.class, () -> RowSourceRecord.builder().row(null));
  }

  @Test
  public void nullTopicName() {
    Assertions.assertThrows(
        NullPointerException.class, () -> RowSourceRecord.builder().topicKey(null));
  }

  @Test
  public void nullSourcePartition() {
    Assertions.assertThrows(
        NullPointerException.class, () -> RowSourceRecord.builder().sourcePartition(null));
  }

  @Test
  public void nullSourceOffset() {
    Assertions.assertThrows(
        NullPointerException.class, () -> RowSourceRecord.builder().sourcePartition(null));
  }

  @Test
  public void testBuilderWithDefaultValue() {
    Row row = Row.of(Cell.of(CommonUtils.randomString(10), 123));
    TopicKey topic = TopicKey.of("g", "n");

    RowSourceRecord r = RowSourceRecord.builder().topicKey(topic).row(row).build();
    Assertions.assertEquals(topic, r.topicKey());
    Assertions.assertEquals(row, r.row());
    Assertions.assertFalse(r.partition().isPresent());
    Assertions.assertFalse(r.timestamp().isPresent());
    Assertions.assertTrue(r.sourceOffset().isEmpty());
    Assertions.assertTrue(r.sourcePartition().isEmpty());
  }

  @Test
  public void testBuilder() {
    Row row = Row.of(Cell.of(CommonUtils.randomString(10), 123));
    TopicKey topic = TopicKey.of("g", "n");
    long ts = CommonUtils.current();
    int partition = 123;
    Map<String, String> sourceOffset = Map.of("abc", "ddd");
    Map<String, String> sourcePartition = Map.of("abc", "ddd");

    RowSourceRecord r =
        RowSourceRecord.builder()
            .topicKey(topic)
            .row(row)
            .timestamp(ts)
            .partition(partition)
            .sourceOffset(sourceOffset)
            .sourcePartition(sourcePartition)
            .build();
    Assertions.assertEquals(topic, r.topicKey());
    Assertions.assertEquals(row, r.row());
    Assertions.assertEquals(ts, (long) r.timestamp().get());
    Assertions.assertEquals(partition, (int) r.partition().get());
    Assertions.assertEquals(sourceOffset, r.sourceOffset());
    Assertions.assertEquals(sourcePartition, r.sourcePartition());
  }

  @Test
  public void failedToModifySourcePartition() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            RowSourceRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .build()
                .sourceOffset()
                .remove("a"));
  }

  @Test
  public void failedToModifySourceOffset() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            RowSourceRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .build()
                .sourceOffset()
                .remove("a"));
  }

  @Test
  public void testCachedRecords() {
    RowSourceRecord record =
        RowSourceRecord.builder()
            .row(Row.of(Cell.of(CommonUtils.randomString(), CommonUtils.randomString())))
            .topicKey(TopicKey.of("g", "n"))
            .build();
    RowSourceTask task =
        new DumbSourceTask() {
          @Override
          protected List<RowSourceRecord> pollRecords() {
            return List.of(record);
          }
        };
    task.start(
        ConnectorFormatter.of()
            .connectorKey(ConnectorKey.of("a", "b"))
            .checkRule(SettingDef.CheckRule.PERMISSIVE)
            .raw());
    Assertions.assertEquals(1, task.poll().size());
    Assertions.assertEquals(1, task.cachedRecords.size());
    org.apache.kafka.clients.producer.RecordMetadata meta =
        new org.apache.kafka.clients.producer.RecordMetadata(
            new org.apache.kafka.common.TopicPartition(TopicKey.of("g", "n").topicNameOnKafka(), 1),
            1,
            2,
            3,
            5,
            6);
    // snapshot the cache and then generate fake records
    var kafkaRecords =
        task.cachedRecords.keySet().stream()
            .map(
                index -> {
                  var header = Mockito.mock(Header.class);
                  Mockito.when(header.value()).thenReturn(ByteUtils.toBytes(index));
                  var headers = Mockito.mock(Headers.class);
                  Mockito.when(headers.lastWithName(RowSourceTask.RECORD_INDEX_KEY))
                      .thenReturn(header);
                  var r = Mockito.mock(SourceRecord.class);
                  Mockito.when(r.headers()).thenReturn(headers);
                  return r;
                })
            .collect(Collectors.toList());
    kafkaRecords.forEach(r -> task.commitRecord(r, meta));
    Assertions.assertEquals(0, task.cachedRecords.size());
  }
}
