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

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_ENCODE_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_NEED_HEADER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FLUSH_SIZE_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.OUTPUT_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.ROTATE_INTERVAL_MS_KEY;
import static oharastream.ohara.kafka.connector.json.ConnectorDefUtils.COLUMNS_DEFINITION;

import java.util.List;
import java.util.Map;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.PropGroup;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.TaskSetting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCsvSinkConfig extends OharaTest {

  private static CsvSinkConfig config(String key, String value) {
    return CsvSinkConfig.of(TaskSetting.of(Map.of(key, value)));
  }

  @Test
  public void testEncode() {
    CsvSinkConfig config = config(FILE_ENCODE_KEY, "10");
    Assertions.assertEquals(config.encode(), "10");
  }

  @Test
  public void testColumns() {
    Column column =
        Column.builder()
            .name(CommonUtils.randomString())
            .dataType(DataType.BOOLEAN)
            .order(1)
            .build();
    CsvSinkConfig config =
        config(COLUMNS_DEFINITION.key(), PropGroup.ofColumn(column).toJsonString());
    Assertions.assertEquals(config.columns(), List.of(column));
  }

  @Test
  public void testFlushSize() {
    CsvSinkConfig config = config(FLUSH_SIZE_KEY, "10");
    Assertions.assertEquals(config.flushSize(), 10);
  }

  @Test
  public void testRotateIntervalMs() {
    CsvSinkConfig config = config(ROTATE_INTERVAL_MS_KEY, "10");
    Assertions.assertEquals(config.rotateIntervalMs(), 10);
  }

  @Test
  public void testOutputFolder() {
    CsvSinkConfig config = config(OUTPUT_FOLDER_KEY, "10");
    Assertions.assertEquals(config.outputFolder(), "10");
  }

  @Test
  public void testHeader() {
    CsvSinkConfig config = config(FILE_NEED_HEADER_KEY, "false");
    Assertions.assertFalse(config.needHeader());
  }
}
