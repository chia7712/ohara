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

import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRowSinkRecord extends OharaTest {

  @Test
  public void nullTopic() {
    Assertions.assertThrows(
        NullPointerException.class, () -> RowSinkRecord.builder().topicKey(null));
  }

  @Test
  public void nullRow() {
    Assertions.assertThrows(NullPointerException.class, () -> RowSinkRecord.builder().row(null));
  }

  @Test
  public void requireTopic() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSinkRecord.builder()
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .timestamp(CommonUtils.current())
                .partition(123)
                .timestampType(TimestampType.NO_TIMESTAMP_TYPE)
                .offset(123)
                .build());
  }

  @Test
  public void requireRow() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSinkRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .timestamp(CommonUtils.current())
                .partition(123)
                .timestampType(TimestampType.NO_TIMESTAMP_TYPE)
                .offset(123)
                .build());
  }

  @Test
  public void requireTimestamp() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSinkRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .partition(123)
                .timestampType(TimestampType.NO_TIMESTAMP_TYPE)
                .offset(123)
                .build());
  }

  @Test
  public void requirePartition() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSinkRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .timestamp(CommonUtils.current())
                .timestampType(TimestampType.NO_TIMESTAMP_TYPE)
                .offset(123)
                .build());
  }

  @Test
  public void requireTimestampType() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSinkRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .timestamp(CommonUtils.current())
                .partition(123)
                .offset(123)
                .build());
  }

  @Test
  public void requireOffset() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            RowSinkRecord.builder()
                .topicKey(TopicKey.of("g", "n"))
                .row(Row.of(Cell.of(CommonUtils.randomString(10), 123)))
                .timestamp(CommonUtils.current())
                .partition(123)
                .timestampType(TimestampType.NO_TIMESTAMP_TYPE)
                .build());
  }

  @Test
  public void testBuilder() {
    Row row = Row.of(Cell.of(CommonUtils.randomString(10), 123));
    TopicKey topic = TopicKey.of("g", "n");
    long ts = CommonUtils.current();
    int partition = 123;
    long offset = 12345;
    TimestampType tsType = TimestampType.NO_TIMESTAMP_TYPE;

    RowSinkRecord r =
        RowSinkRecord.builder()
            .topicKey(topic)
            .row(row)
            .timestamp(ts)
            .partition(partition)
            .timestampType(tsType)
            .offset(offset)
            .build();
    Assertions.assertEquals(topic, r.topicKey());
    Assertions.assertEquals(row, r.row());
    Assertions.assertEquals(ts, r.timestamp());
    Assertions.assertEquals(partition, r.partition());
    Assertions.assertEquals(tsType, r.timestampType());
    Assertions.assertEquals(offset, r.offset());
  }
}
