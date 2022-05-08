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

package oharastream.ohara.connector.smb

import oharastream.ohara.client.filesystem
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.kafka.connector.csv.CsvSinkTask
import oharastream.ohara.kafka.connector.storage.FileSystem

class SmbSinkTask extends CsvSinkTask {
  override def fileSystem(settings: TaskSetting): FileSystem = {
    val props = SmbProps(settings)
    filesystem.FileSystem.smbBuilder
      .hostname(props.hostname)
      .port(props.port)
      .user(props.user)
      .password(props.password)
      .shareName(props.shareName)
      .build()
  }
}
