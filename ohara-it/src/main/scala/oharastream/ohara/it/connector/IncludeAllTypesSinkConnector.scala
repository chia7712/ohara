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

package oharastream.ohara.it.connector

import java.util

import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.kafka.connector.{RowSinkConnector, RowSinkTask, TaskSetting}

import scala.jdk.CollectionConverters._

/**
  * This is a stupid and do-nothing connector. It burns for testing deploy custom connector.
  * It is not placed at test scope since we need jar when tests manually.
  */
class IncludeAllTypesSinkConnector extends RowSinkConnector {
  private[this] var settings: TaskSetting                                         = _
  override protected def run(settings: TaskSetting): Unit                         = this.settings = settings
  override protected def terminate(): Unit                                        = {}
  override protected def taskClass(): Class[_ <: RowSinkTask]                     = classOf[IncludeAllTypesSinkTask]
  override protected def taskSettings(maxTasks: Int): util.List[TaskSetting]      = Seq.fill(maxTasks)(settings).asJava
  override protected def customSettingDefinitions(): util.Map[String, SettingDef] = ALL_SETTING_DEFINITIONS.asJava
}
