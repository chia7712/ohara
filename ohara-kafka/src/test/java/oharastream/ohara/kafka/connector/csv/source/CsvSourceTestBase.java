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

package oharastream.ohara.kafka.connector.csv.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.RowSourceContext;
import oharastream.ohara.kafka.connector.RowSourceRecord;
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions;
import org.junit.jupiter.api.Assertions;

public abstract class CsvSourceTestBase extends OharaTest {
  protected static final TopicKey TOPIC = TopicKey.of("test", "topic");
  protected static final int TASK_TOTAL = 1;
  protected static final int TASK_HASH = 0;

  protected static final List<Column> SCHEMA =
      Arrays.asList(
          Column.builder().name("hostname").dataType(DataType.STRING).build(),
          Column.builder().name("port").dataType(DataType.INT).build(),
          Column.builder().name("running").dataType(DataType.BOOLEAN).build());

  protected static final List<Row> VERIFICATION_DATA = setupVerificationData();
  protected static final List<String> INPUT_DATA = setupInputData();

  protected Map<String, String> props;
  protected RowSourceContext rowContext;

  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(CsvConnectorDefinitions.TASK_TOTAL_KEY, String.valueOf(TASK_TOTAL));
    props.put(CsvConnectorDefinitions.TASK_HASH_KEY, String.valueOf(TASK_HASH));
    props.put("topics", TOPIC.topicNameOnKafka());
    props.put("topicKeys", TopicKey.toJsonString(Set.of(TOPIC)));
    return props;
  }

  protected void setup() {
    rowContext = FakeSourceContext.of();
    props = createProps();
  }

  protected static List<Row> setupVerificationData() {
    int size = 3;
    return IntStream.range(1, size)
        .mapToObj(
            i ->
                Row.of(
                    Cell.of(SCHEMA.get(0).name(), CommonUtils.randomString()),
                    Cell.of(SCHEMA.get(1).name(), 1024 + i),
                    Cell.of(SCHEMA.get(2).name(), i % 2 == 0)))
        .collect(Collectors.toUnmodifiableList());
  }

  protected static List<String> setupInputData() {
    return VERIFICATION_DATA.stream()
        .map(
            row ->
                row.cells().stream()
                    .map(cell -> cell.value().toString())
                    .collect(Collectors.joining(",")))
        .collect(Collectors.toUnmodifiableList());
  }

  protected void verifyRecords(List<RowSourceRecord> records) {
    Assertions.assertEquals(VERIFICATION_DATA.size(), records.size());
    for (RowSourceRecord record : records) {
      Assertions.assertEquals(TOPIC, record.topicKey());
    }
    verifyRows(VERIFICATION_DATA, extractRow(records));
  }

  protected void verifyRows(List<Row> expected, List<Row> actual) {
    Assertions.assertEquals(expected.size(), actual.size());
    for (int index = 0; index < expected.size(); index++) {
      Assertions.assertEquals(expected.get(index), actual.get(index));
    }
  }

  protected List<Row> extractRow(List<RowSourceRecord> records) {
    return records.stream().map(RowSourceRecord::row).collect(Collectors.toUnmodifiableList());
  }
}
