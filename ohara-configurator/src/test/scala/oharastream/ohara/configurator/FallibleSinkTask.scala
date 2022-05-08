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

package oharastream.ohara.configurator

import oharastream.ohara.kafka.connector.{RowSinkRecord, RowSinkTask, TaskSetting}

class FallibleSinkTask extends RowSinkTask {
  override protected def run(settings: TaskSetting): Unit = {
    if (settings.stringOption(FallibleSinkTask.FAILURE_FLAG).isPresent)
      throw new IllegalArgumentException("Someone hate me...")
  }

  override protected def terminate(): Unit = {}

  override protected def putRecords(records: java.util.List[RowSinkRecord]): Unit = {}
}

object FallibleSinkTask {
  val FAILURE_FLAG = "task_should_fail"
}
