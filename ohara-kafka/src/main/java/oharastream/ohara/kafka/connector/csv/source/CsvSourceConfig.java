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

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.COMPLETED_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.ERROR_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_ENCODE_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_ENCODE_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.INPUT_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.SIZE_OF_FILE_CACHE_DEFAULT;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.SIZE_OF_FILE_CACHE_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.TASK_HASH_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.TASK_TOTAL_KEY;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.kafka.connector.TaskSetting;

public interface CsvSourceConfig {

  /** @return the count of tasks */
  int total();

  /** @return the hash of this task */
  int hash();

  /** @return the max number of processed lines in each poll */
  int maximumNumberOfLines();

  /** @return the folder containing the csv files */
  String inputFolder();

  /** @return size of file cache */
  int fileCacheSize();

  /** @return the folder storing the processed csv files */
  Optional<String> completedFolder();

  /** @return the folder storing the corrupt csv files */
  Optional<String> errorFolder();

  /** @return the string encode to parse csv files */
  String encode();

  /** @return target topics */
  Set<TopicKey> topicKeys();

  /** @return the rules to control the output records */
  List<Column> columns();

  static CsvSourceConfig of(TaskSetting setting) {
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
  static CsvSourceConfig of(TaskSetting setting, List<Column> columns) {
    return new CsvSourceConfig() {

      @Override
      public int total() {
        return setting.intValue(TASK_TOTAL_KEY);
      }

      @Override
      public int hash() {
        return setting.intValue(TASK_HASH_KEY);
      }

      @Override
      public int maximumNumberOfLines() {
        return setting
            .intOption(MAXIMUM_NUMBER_OF_LINES_KEY)
            .orElse(MAXIMUM_NUMBER_OF_LINES_DEFAULT);
      }

      @Override
      public String inputFolder() {
        return setting.stringValue(INPUT_FOLDER_KEY);
      }

      @Override
      public int fileCacheSize() {
        return setting.intOption(SIZE_OF_FILE_CACHE_KEY).orElse(SIZE_OF_FILE_CACHE_DEFAULT);
      }

      @Override
      public Optional<String> completedFolder() {
        return setting.stringOption(COMPLETED_FOLDER_KEY);
      }

      @Override
      public Optional<String> errorFolder() {
        return setting.stringOption(ERROR_FOLDER_KEY);
      }

      @Override
      public String encode() {
        // We fulfil the auto-complete for the default value to simplify our UT
        // BTW, the default value is handled by Configurator :)
        return setting.stringOption(FILE_ENCODE_KEY).orElse(FILE_ENCODE_DEFAULT);
      }

      @Override
      public Set<TopicKey> topicKeys() {
        return setting.topicKeys();
      }

      @Override
      public List<Column> columns() {
        return Collections.unmodifiableList(columns);
      }
    };
  }
}
