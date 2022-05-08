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

package oharastream.ohara.it.performance

import oharastream.ohara.client.filesystem.FileSystem

private[performance] object PerformanceTestingUtils {
  val INPUTDATA_TIMEOUT_KEY: String    = "ohara.it.performance.input.data.timeout"
  val DURATION_KEY: String             = "ohara.it.performance.duration"
  val REPORT_OUTPUT_KEY: String        = "ohara.it.performance.report.output"
  val LOG_METERS_FREQUENCY_KEY: String = "ohara.it.performance.log.meters.frequency"
  val DATA_SIZE_KEY: String            = "ohara.it.performance.data.size"
  val PARTITION_SIZE_KEY: String       = "ohara.it.performance.topic.partitions"
  val TASK_SIZE_KEY: String            = "ohara.it.performance.connector.tasks"
  val ROW_FLUSH_NUMBER_KEY: String     = "ohara.it.performance.row.flush.number"
  val FILENAME_CACHE_SIZE_KEY: String  = "ohara.it.performance.filename.cache.size"
  // HDFS Setting Key
  val dataDir: String = "/tmp"

  val CSV_FILE_FLUSH_SIZE_KEY: String = "ohara.it.performance.csv.file.flush.size"
  val CSV_INPUT_KEY: String           = "ohara.it.performance.csv.input"

  val DATA_CLEANUP_KEY: String = "ohara.it.performance.cleanup"

  def createFolder(fileSystem: FileSystem, path: String): String = {
    if (fileSystem.nonExists(path)) fileSystem.mkdirs(path)
    path
  }

  def deleteFolder(fileSystem: FileSystem, path: String): Unit = {
    if (fileSystem.exists(path)) fileSystem.delete(path, true)
  }

  def exists(fileSystem: FileSystem, path: String): Boolean = {
    fileSystem.exists(path)
  }
}
