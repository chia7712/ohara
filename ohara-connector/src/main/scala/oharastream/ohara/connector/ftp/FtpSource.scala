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
import oharastream.ohara.kafka.connector.csv.CsvSourceConnector
import oharastream.ohara.kafka.connector.{RowSourceTask, TaskSetting, storage}

import scala.jdk.CollectionConverters._

class FtpSource extends CsvSourceConnector {
  override def fileSystem(config: TaskSetting): storage.FileSystem = {
    val props: FtpSourceProps = FtpSourceProps(config)
    FileSystem.ftpBuilder.hostname(props.hostname).port(props.port).user(props.user).password(props.password).build()
  }
  private[this] var settings: TaskSetting = _

  override protected def taskClass(): Class[_ <: RowSourceTask] = classOf[FtpSourceTask]

  override protected def csvTaskSettings(maxTasks: Int): util.List[TaskSetting] = Seq.fill(maxTasks)(settings).asJava

  override protected[ftp] def execute(settings: TaskSetting): Unit = this.settings = settings

  override protected def terminate(): Unit = {
    //    do nothing
  }

  override protected def csvSettingDefinitions(): util.Map[String, SettingDef] = DEFINITIONS.asJava
}
