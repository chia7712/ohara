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
import java.util.NoSuchElementException;
import oharastream.ohara.common.data.*;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ConnectorKey;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.json.ConnectorFormatter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConnectorProps extends OharaTest {

  @Test
  public void emptyInStartingSource() {
    DumbSource connector = new DumbSource();
    // ohara auto-fill the keys with default values
    connector.start(Map.of());
  }

  @Test
  public void emptyInStartingSink() {
    DumbSink connector = new DumbSink();
    // ohara auto-fill the keys with default values
    connector.start(Map.of());
  }

  @Test
  public void emptyInStartingSourceTask() {
    DumbSourceTask task = new DumbSourceTask();
    Assertions.assertThrows(NoSuchElementException.class, () -> task.start(Map.of()));
  }

  @Test
  public void emptyInStartingSinkTask() {
    DumbSinkTask task = new DumbSinkTask();
    Assertions.assertThrows(NoSuchElementException.class, () -> task.start(Map.of()));
  }

  @Test
  public void testCounterInSink() {
    Column column =
        Column.builder()
            .name(CommonUtils.randomString(10))
            .dataType(DataType.STRING)
            .order(1)
            .build();
    RowSinkTask task = new DumbSinkTask();
    ConnectorKey connectorKey = ConnectorKey.of("g", "n");
    // we call start to initialize counter.
    task.start(
        ConnectorFormatter.of()
            .connectorKey(connectorKey)
            .checkRule(SettingDef.CheckRule.PERMISSIVE)
            .column(column)
            .raw());
    try {
      Assertions.assertNotNull(task.messageNumberCounter);
      Assertions.assertNotNull(task.messageSizeCounter);
      Assertions.assertNotNull(task.ignoredMessageSizeCounter);
      Assertions.assertNotNull(task.ignoredMessageNumberCounter);

      Assertions.assertEquals(task.messageNumberCounter.key(), connectorKey);
      Assertions.assertEquals(task.messageSizeCounter.key(), connectorKey);
      Assertions.assertEquals(task.ignoredMessageSizeCounter.key(), connectorKey);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.key(), connectorKey);

      Assertions.assertEquals(task.messageNumberCounter.getValue(), 0);
      Assertions.assertEquals(task.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.getValue(), 0);

      // add legal data
      task.put(
          List.of(
              new SinkRecord(
                  TopicKey.of("g", "n").topicNameOnKafka(),
                  0,
                  null,
                  Serializer.ROW.to(Row.of(Cell.of(column.name(), CommonUtils.randomString()))),
                  null,
                  null,
                  10)));
      Assertions.assertEquals(task.messageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageSizeCounter.getValue(), 0);

      // add illegal data
      task.put(
          List.of(
              new SinkRecord(
                  TopicKey.of("g", "n").topicNameOnKafka(),
                  0,
                  null,
                  Serializer.ROW.to(Row.of(Cell.of(column.name(), 12313))),
                  null,
                  null,
                  10)));
      Assertions.assertEquals(task.messageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task.ignoredMessageSizeCounter.getValue(), 0);

    } finally {
      task.stop();
      Assertions.assertTrue(task.messageNumberCounter.isClosed());
      Assertions.assertTrue(task.messageSizeCounter.isClosed());
      Assertions.assertTrue(task.ignoredMessageSizeCounter.isClosed());
      Assertions.assertTrue(task.ignoredMessageNumberCounter.isClosed());
    }

    RowSinkTask task2 = new DumbSinkTask();
    try {
      // we call start to initialize counter.
      task2.start(
          ConnectorFormatter.of()
              .connectorKey(ConnectorKey.of("g", "n"))
              .checkRule(SettingDef.CheckRule.ENFORCING)
              .column(column)
              .raw());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              task2.put(
                  List.of(
                      new SinkRecord(
                          "topic",
                          0,
                          null,
                          Row.of(Cell.of(column.name(), 12313)),
                          null,
                          null,
                          10))));
    } finally {
      task2.stop();
    }
  }

  @Test
  public void testCounterInSource() {
    Column column =
        Column.builder()
            .name(CommonUtils.randomString(10))
            .dataType(DataType.STRING)
            .order(1)
            .build();
    Row goodRow = Row.of(Cell.of(column.newName(), CommonUtils.randomString()));
    Row badRow = Row.of(Cell.of(column.newName(), 123123));
    RowSourceTask task =
        new DumbSourceTask() {
          private boolean good = true;

          @Override
          protected List<RowSourceRecord> pollRecords() {
            try {
              return List.of(
                  RowSourceRecord.builder()
                      .row(good ? goodRow : badRow)
                      .topicKey(TopicKey.of("g", CommonUtils.randomString(10)))
                      .build());
            } finally {
              good = false;
            }
          }
        };
    ConnectorKey connectorKey = ConnectorKey.of("g", "n");
    // we call start to initialize counter.
    task.start(
        ConnectorFormatter.of()
            .connectorKey(connectorKey)
            .checkRule(SettingDef.CheckRule.PERMISSIVE)
            .column(column)
            .raw());
    try {
      Assertions.assertNotNull(task.messageNumberCounter);
      Assertions.assertNotNull(task.messageSizeCounter);
      Assertions.assertNotNull(task.ignoredMessageNumberCounter);
      Assertions.assertNotNull(task.ignoredMessageSizeCounter);

      Assertions.assertEquals(task.messageNumberCounter.key(), connectorKey);
      Assertions.assertEquals(task.messageSizeCounter.key(), connectorKey);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.key(), connectorKey);
      Assertions.assertEquals(task.ignoredMessageSizeCounter.key(), connectorKey);

      Assertions.assertEquals(task.messageNumberCounter.getValue(), 0);
      Assertions.assertEquals(task.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageSizeCounter.getValue(), 0);

      task.poll();
      Assertions.assertEquals(task.messageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageSizeCounter.getValue(), 0);

      // this poll generates bad data
      task.poll();
      Assertions.assertEquals(task.messageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task.ignoredMessageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task.ignoredMessageSizeCounter.getValue(), 0);

    } finally {
      task.stop();
      Assertions.assertTrue(task.messageNumberCounter.isClosed());
      Assertions.assertTrue(task.messageSizeCounter.isClosed());
      Assertions.assertTrue(task.ignoredMessageNumberCounter.isClosed());
      Assertions.assertTrue(task.ignoredMessageSizeCounter.isClosed());
    }

    RowSourceTask task2 =
        new DumbSourceTask() {
          @Override
          protected List<RowSourceRecord> pollRecords() {
            return List.of(
                RowSourceRecord.builder()
                    .row(badRow)
                    .topicKey(TopicKey.of("g", CommonUtils.randomString(10)))
                    .build());
          }
        };

    try {
      // we call start to initialize counter.
      task2.start(
          ConnectorFormatter.of()
              .connectorKey(connectorKey)
              .checkRule(SettingDef.CheckRule.ENFORCING)
              .column(column)
              .raw());
      // this poll generates bad data and the check rule is "enforcing"
      Assertions.assertThrows(IllegalArgumentException.class, task2::poll);
    } finally {
      task2.stop();
    }

    RowSourceTask task3 =
        new DumbSourceTask() {
          @Override
          protected List<RowSourceRecord> pollRecords() {
            return List.of(
                RowSourceRecord.builder()
                    .row(badRow)
                    .topicKey(TopicKey.of("g", CommonUtils.randomString(10)))
                    .build());
          }
        };

    try {
      // we call start to initialize counter.
      task3.start(
          ConnectorFormatter.of()
              .connectorKey(connectorKey)
              .checkRule(SettingDef.CheckRule.NONE)
              .column(column)
              .raw());
      // this poll generates bad data and the check rule is "enforcing"
      task3.poll();
      Assertions.assertEquals(task3.messageNumberCounter.getValue(), 1);
      Assertions.assertNotEquals(task3.messageSizeCounter.getValue(), 0);
      Assertions.assertEquals(task3.ignoredMessageNumberCounter.getValue(), 0);
      Assertions.assertEquals(task3.ignoredMessageSizeCounter.getValue(), 0);
    } finally {
      task3.stop();
    }
  }

  @Test
  public void testStop() {
    RowSourceTask task = new DumbSourceTask();
    // we don't call start() so all internal counters should be null
    task.stop();
  }

  @Test
  public void testInternalTaskConfigOfSource() {
    RowSourceConnector connector = new DumbSource();
    Assertions.assertNull(connector.taskSetting);
    connector.start(ConnectorFormatter.of().connectorKey(ConnectorKey.of("g", "n")).raw());
    Assertions.assertNotNull(connector.taskSetting);
  }

  @Test
  public void testInternalTaskConfigOfSourceTask() {
    RowSourceTask task = new DumbSourceTask();
    Assertions.assertNull(task.taskSetting);

    task.start(ConnectorFormatter.of().connectorKey(ConnectorKey.of("g", "n")).raw());
    Assertions.assertNotNull(task.taskSetting);
  }

  @Test
  public void testInternalObjectKeyOfSourceTask() {
    RowSourceTask task = new DumbSourceTask();
    Assertions.assertNull(task.keyInBytes);

    task.start(ConnectorFormatter.of().connectorKey(ConnectorKey.of("g", "n")).raw());
    Assertions.assertNotNull(task.keyInBytes);
  }

  @Test
  public void testInternalTaskConfigOfSink() {
    RowSinkConnector connector = new DumbSink();
    Assertions.assertNull(connector.taskSetting);

    connector.start(ConnectorFormatter.of().connectorKey(ConnectorKey.of("g", "n")).raw());
    Assertions.assertNotNull(connector.taskSetting);
  }

  @Test
  public void testInternalTaskConfigOfSinkTask() {
    RowSinkTask task = new DumbSinkTask();
    Assertions.assertNull(task.taskSetting);

    task.start(ConnectorFormatter.of().connectorKey(ConnectorKey.of("g", "n")).raw());
    Assertions.assertNotNull(task.taskSetting);
  }

  @Test
  public void failToCallCounterBuilderBeforeStartingSource() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DumbSource().counterBuilder());
  }

  @Test
  public void failToCallCounterBuilderBeforeStartingSink() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new DumbSink().counterBuilder());
  }

  @Test
  public void failToCallCounterBuilderBeforeStartingSourceTask() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DumbSourceTask().counterBuilder());
  }

  @Test
  public void failToCallCounterBuilderBeforeStartingSinkTask() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new DumbSinkTask().counterBuilder());
  }
}
