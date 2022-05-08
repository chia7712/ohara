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

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_ENCODE_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_ENCODE_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_NEED_HEADER_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_NEED_HEADER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FLUSH_SIZE_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FLUSH_SIZE_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.OUTPUT_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.ROTATE_INTERVAL_MS_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.ROTATE_INTERVAL_MS_KEY;

import java.util.Collections;
import java.util.List;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.kafka.connector.TaskSetting;

/** This class is used to define the configuration of CsvSinkTask. */
public interface CsvSinkConfig {

  /** @return flushSize */
  int flushSize();

  /** @return the time to rotate the output */
  long rotateIntervalMs();

  /** @return the folder to write csv output */
  String outputFolder();

  /** @return the string encode to parse csv files */
  String encode();

  /** @return true if the output csv needs header. otherwise, false */
  boolean needHeader();

  /** @return the rules to control the output records */
  List<Column> columns();

  static CsvSinkConfig of(TaskSetting setting) {
    return of(setting, setting.columns());
  }

  /**
   * this method enable us to override the columns used in csv job.
   *
   * @param setting task settings
   * @param columns new columns
   * @return csv configs
   */
  @VisibleForTesting
  static CsvSinkConfig of(TaskSetting setting, List<Column> columns) {
    return new CsvSinkConfig() {

      @Override
      public int flushSize() {
        // We fulfil the auto-complete for the default value to simplify our UT
        // BTW, the default value is handled by Configurator :)
        return setting.intOption(FLUSH_SIZE_KEY).orElse(FLUSH_SIZE_DEFAULT);
      }

      @Override
      public long rotateIntervalMs() {
        // We fulfil the auto-complete for the default value to simplify our UT
        // BTW, the default value is handled by Configurator :)
        return setting.longOption(ROTATE_INTERVAL_MS_KEY).orElse(ROTATE_INTERVAL_MS_DEFAULT);
      }

      @Override
      public String outputFolder() {
        return setting.stringValue(OUTPUT_FOLDER_KEY);
      }

      @Override
      public String encode() {
        // We fulfil the auto-complete for the default value to simplify our UT
        // BTW, the default value is handled by Configurator :)
        return setting.stringOption(FILE_ENCODE_KEY).orElse(FILE_ENCODE_DEFAULT);
      }

      @Override
      public boolean needHeader() {
        // We fulfil the auto-complete for the default value to simplify our UT
        // BTW, the default value is handled by Configurator :)
        return setting.booleanOption(FILE_NEED_HEADER_KEY).orElse(FILE_NEED_HEADER_DEFAULT);
      }

      @Override
      public List<Column> columns() {
        return Collections.unmodifiableList(columns);
      }
    };
  }
}
