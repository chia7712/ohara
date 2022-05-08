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

package oharastream.ohara.connector.hdfs.sink

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.kafka.connector.csv.CsvSinkTask
import oharastream.ohara.kafka.connector.{TaskSetting, storage}

class HDFSSinkTask extends CsvSinkTask {
  override def fileSystem(setting: TaskSetting): storage.FileSystem =
    FileSystem.hdfsBuilder
      .url(HDFSSinkProps(setting).hdfsURL)
      .replicationNumber(HDFSSinkProps(setting).replicationNumber)
      .build
}
