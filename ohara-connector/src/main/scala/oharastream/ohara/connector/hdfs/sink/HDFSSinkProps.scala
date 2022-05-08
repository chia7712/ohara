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

import oharastream.ohara.kafka.connector.TaskSetting

case class HDFSSinkProps(hdfsURL: String, replicationNumber: Int) {
  def toMap: Map[String, String] =
    Map(HDFS_URL_KEY -> hdfsURL, HDFS_REPLICATION_NUMBER_KEY -> replicationNumber.toString())
}

object HDFSSinkProps {
  val FS_DEFAULT: String = "fs.defaultFS"
  def apply(settings: TaskSetting): HDFSSinkProps = {
    HDFSSinkProps(
      hdfsURL = settings.stringValue(HDFS_URL_KEY),
      replicationNumber = settings.intOption(HDFS_REPLICATION_NUMBER_KEY).orElse(HDFS_REPLICATION_NUMBER_DEFAULT)
    )
  }
}
