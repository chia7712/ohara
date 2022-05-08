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

package oharastream.ohara.connector.ftp

import java.util

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.kafka.connector._
import oharastream.ohara.kafka.connector.csv.CsvSinkConnector

import scala.jdk.CollectionConverters._

class FtpSink extends CsvSinkConnector {
  private[this] var settings: TaskSetting = _

  override def fileSystem(setting: TaskSetting): storage.FileSystem = {
    val props = FtpSinkProps(setting)
    FileSystem.ftpBuilder.hostname(props.hostname).port(props.port).user(props.user).password(props.password).build()
  }

  override protected def execute(settings: TaskSetting): Unit = this.settings = settings

  override protected def terminate(): Unit = {
    // do nothing
  }

  override protected def taskClass(): Class[_ <: RowSinkTask] = classOf[FtpSinkTask]

  override protected def csvTaskSettings(maxTasks: Int): util.List[TaskSetting] = Seq.fill(maxTasks)(settings).asJava

  override protected def csvSettingDefinitions(): util.Map[String, SettingDef] = DEFINITIONS.asJava
}
