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

package oharastream.ohara.connector.console

import java.util

import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.kafka.connector.{RowSinkConnector, RowSinkTask, TaskSetting}

import scala.jdk.CollectionConverters._

class ConsoleSink extends RowSinkConnector {
  private[this] var config: TaskSetting                 = _
  override protected def run(config: TaskSetting): Unit = this.config = config

  override protected def terminate(): Unit = {
    // do nothing
  }

  override protected def taskClass(): Class[_ <: RowSinkTask] = classOf[ConsoleSinkTask]

  override protected def taskSettings(maxTasks: Int): util.List[TaskSetting] = Seq.fill(maxTasks)(config).asJava

  override protected def customSettingDefinitions(): util.Map[String, SettingDef] =
    Map(
      CONSOLE_FREQUENCE_DEFINITION.key()   -> CONSOLE_FREQUENCE_DEFINITION,
      CONSOLE_ROW_DIVIDER_DEFINITION.key() -> CONSOLE_ROW_DIVIDER_DEFINITION,
      CONSOLE_CHECK_RULE_DEFINITION.key()  -> CONSOLE_CHECK_RULE_DEFINITION
    ).asJava
}
