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

import java.util.concurrent.atomic.AtomicInteger;
import oharastream.ohara.common.setting.SettingDef;

/** this class maintains all available definitions for both csv source and csv sink. */
public final class CsvConnectorDefinitions {
  /** used to set the "order" for definitions. */
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  public static final String INPUT_FOLDER_KEY = "input.folder";
  public static final SettingDef INPUT_FOLDER_DEFINITION =
      SettingDef.builder()
          .displayName("Input Folder")
          .documentation("Connector will load csv file from this folder")
          .required(SettingDef.Type.STRING)
          .key(INPUT_FOLDER_KEY)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String COMPLETED_FOLDER_KEY = "completed.folder";
  public static final SettingDef COMPLETED_FOLDER_DEFINITION =
      SettingDef.builder()
          .displayName("Completed Folder")
          .documentation("This folder is used to store the completed files")
          .optional(SettingDef.Type.STRING)
          .key(COMPLETED_FOLDER_KEY)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String ERROR_FOLDER_KEY = "error.folder";
  public static final SettingDef ERROR_FOLDER_DEFINITION =
      SettingDef.builder()
          .displayName("Error Folder")
          .documentation("This folder is used to keep the invalid files. For example, non-csv file")
          .optional(SettingDef.Type.STRING)
          .key(ERROR_FOLDER_KEY)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String OUTPUT_FOLDER_KEY = "output.folder";
  public static final SettingDef OUTPUT_FOLDER_DEFINITION =
      SettingDef.builder()
          .displayName("Output Folder")
          .documentation("Read csv data from topic and then write to this folder")
          .required(SettingDef.Type.STRING)
          .key(OUTPUT_FOLDER_KEY)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String FLUSH_SIZE_KEY = "flush.size";
  public static final int FLUSH_SIZE_DEFAULT = 1000;
  public static final SettingDef FLUSH_SIZE_DEFINITION =
      SettingDef.builder()
          .displayName("Flush Size")
          .documentation("Number of records write to store before invoking file commits")
          .optional(FLUSH_SIZE_DEFAULT)
          .key(FLUSH_SIZE_KEY)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String ROTATE_INTERVAL_MS_KEY = "rotate.interval.ms";
  public static final long ROTATE_INTERVAL_MS_DEFAULT = 60000;
  public static final SettingDef ROTATE_INTERVAL_MS_DEFINITION =
      SettingDef.builder()
          .displayName("Rotate Interval(MS)")
          .documentation("Commit file time")
          .key(ROTATE_INTERVAL_MS_KEY)
          .optional(ROTATE_INTERVAL_MS_DEFAULT)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String FILE_NEED_HEADER_KEY = "file.need.header";
  public static final boolean FILE_NEED_HEADER_DEFAULT = true;
  public static final SettingDef FILE_NEED_HEADER_DEFINITION =
      SettingDef.builder()
          .displayName("File Need Header")
          .documentation("File need header for flush data")
          .optional(FILE_NEED_HEADER_DEFAULT)
          .key(FILE_NEED_HEADER_KEY)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String FILE_ENCODE_KEY = "file.encode";
  public static final String FILE_ENCODE_DEFAULT = "UTF-8";
  public static final SettingDef FILE_ENCODE_DEFINITION =
      SettingDef.builder()
          .displayName("File Encode")
          .documentation("File encode for write to file")
          .key(FILE_ENCODE_KEY)
          .optional(FILE_ENCODE_DEFAULT)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String SIZE_OF_FILE_CACHE_KEY = "size.of.file.cache";
  public static final int SIZE_OF_FILE_CACHE_DEFAULT = 4096;
  public static final SettingDef SIZE_OF_FILE_CACHE_DEFINITION =
      SettingDef.builder()
          .displayName("Size of file cache")
          .documentation(
              "The files listed from remote server are cached in the connector "
                  + " so as to reduce the count of sending expensive list operation.")
          .key(SIZE_OF_FILE_CACHE_KEY)
          .positiveNumber(SIZE_OF_FILE_CACHE_DEFAULT)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String MAXIMUM_NUMBER_OF_LINES_KEY = "maximum.number.of.lines";
  public static final int MAXIMUM_NUMBER_OF_LINES_DEFAULT = 1000;
  public static final SettingDef MAXIMUM_NUMBER_OF_LINES_DEFINITION =
      SettingDef.builder()
          .displayName("Maximum number of lines to be processed")
          .documentation(
              "the max number of lines can be process by each loop. If the csv files is too large, "
                  + "the loop of processing file will take a long time to produce output.")
          .key(MAXIMUM_NUMBER_OF_LINES_KEY)
          .positiveNumber(MAXIMUM_NUMBER_OF_LINES_DEFAULT)
          .orderInGroup(COUNTER.getAndIncrement())
          .build();

  public static final String TASK_TOTAL_KEY = "task.total";
  public static final String TASK_HASH_KEY = "task.hash";

  private CsvConnectorDefinitions() {}
}
