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

/** Provider of a record writer for this file system. */
public interface RecordWriterProvider {

  /**
   * Get the file extension name for this file system.
   *
   * @return file extension name
   */
  String getExtension();

  /**
   * Creates a record writer for this file system.
   *
   * @param config CSV sink configuration
   * @param filePath filePath
   * @return RecordWriter
   */
  RecordWriter getRecordWriter(CsvSinkConfig config, String filePath);
}
