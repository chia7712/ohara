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

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.COMPLETED_FOLDER_DEFINITION;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.ERROR_FOLDER_DEFINITION;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.FILE_ENCODE_DEFINITION;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.INPUT_FOLDER_DEFINITION;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_DEFINITION;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.SIZE_OF_FILE_CACHE_DEFINITION;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.kafka.connector.RowSourceConnector;
import oharastream.ohara.kafka.connector.TaskSetting;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import oharastream.ohara.kafka.connector.storage.FileType;

/**
 * A wrap to RowSourceConnector. The difference between CsvSourceConnector and RowSourceConnector is
 * that CsvSourceConnector already has some default definitions, as follows:
 *
 * <ul>
 *   <li>INPUT_FOLDER_DEFINITION: Connector will load csv file from this folder
 *   <li>COMPLETED_FOLDER_DEFINITION: This folder is used to store the completed files
 *   <li>ERROR_FOLDER_DEFINITION: This folder is used to keep the invalid files
 *   <li>FILE_ENCODE_DEFINITION: File encode for write to file
 * </ul>
 */
public abstract class CsvSourceConnector extends RowSourceConnector {

  /**
   * Return the file system for this connector
   *
   * @param config initial configuration
   * @return a FileSystem implementation
   */
  public abstract FileSystem fileSystem(TaskSetting config);

  /**
   * Return the settings for csv source task.
   *
   * @param maxTasks number of tasks for this connector
   * @return a seq from settings
   */
  protected abstract List<TaskSetting> csvTaskSettings(int maxTasks);

  private static void checkExist(FileSystem fs, String path) {
    if (fs.fileType(path) != FileType.FOLDER)
      throw new IllegalArgumentException(path + " is NOT folder!!!");
  }

  /**
   * execute this connector. Noted: this method is invoked after all csv-related settings are
   * confirmed.
   *
   * @param setting task setting
   */
  protected abstract void execute(TaskSetting setting);

  @Override
  protected final void run(TaskSetting setting) {
    try (FileSystem fileSystem = fileSystem(setting)) {
      checkExist(fileSystem, setting.stringValue(CsvConnectorDefinitions.INPUT_FOLDER_KEY));
      setting
          .stringOption(CsvConnectorDefinitions.COMPLETED_FOLDER_KEY)
          .ifPresent(path -> checkExist(fileSystem, path));
      setting
          .stringOption(CsvConnectorDefinitions.ERROR_FOLDER_KEY)
          .ifPresent(path -> checkExist(fileSystem, path));
    } finally {
      execute(setting);
    }
  }

  @Override
  public final List<TaskSetting> taskSettings(int maxTasks) {
    List<TaskSetting> sub = csvTaskSettings(maxTasks);
    return IntStream.range(0, sub.size())
        .mapToObj(
            index ->
                sub.get(index)
                    .append(
                        Map.of(
                            CsvConnectorDefinitions.TASK_TOTAL_KEY, String.valueOf(sub.size()),
                            CsvConnectorDefinitions.TASK_HASH_KEY, String.valueOf(index))))
        .collect(Collectors.toUnmodifiableList());
  }

  /** @return custom setting definitions from sub csv connectors */
  protected Map<String, SettingDef> csvSettingDefinitions() {
    return Map.of();
  }

  @Override
  protected final Map<String, SettingDef> customSettingDefinitions() {
    Map<String, SettingDef> finalDefinitions = new TreeMap<>(csvSettingDefinitions());
    finalDefinitions.putAll(
        Stream.of(
                SIZE_OF_FILE_CACHE_DEFINITION,
                MAXIMUM_NUMBER_OF_LINES_DEFINITION,
                INPUT_FOLDER_DEFINITION,
                COMPLETED_FOLDER_DEFINITION,
                ERROR_FOLDER_DEFINITION,
                FILE_ENCODE_DEFINITION)
            .collect(Collectors.toUnmodifiableMap(SettingDef::key, Function.identity())));
    return Collections.unmodifiableMap(finalDefinitions);
  }
}
