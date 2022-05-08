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

package oharastream.ohara.client.kafka

import java.util

import oharastream.ohara.kafka.connector.{RowSourceConnector, RowSourceTask, TaskSetting}

class BrokenConnector extends RowSourceConnector {
  override protected def taskClass(): Class[_ <: RowSourceTask] = classOf[BrokenConnectorTask]

  override protected def taskSettings(maxTasks: Int): util.List[TaskSetting] =
    throw new IllegalArgumentException("This is an unrunnable connector")

  override protected def run(settings: TaskSetting): Unit =
    throw new IllegalArgumentException("This is an unrunnable connector")

  override protected def terminate(): Unit = {
    // do nothing
  }
}
