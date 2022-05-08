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

import oharastream.ohara.common.data.{Cell, Row}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

class MyConnectorTask extends RowSourceTask {
  private[this] var lastSent: Long     = 0
  private[this] var topicKey: TopicKey = _

  override protected def run(settings: TaskSetting): Unit = this.topicKey = settings.topicKeys().iterator().next

  override protected def terminate(): Unit = {
    // do nothing
  }

  override protected def pollRecords(): util.List[RowSourceRecord] = {
    val current = System.currentTimeMillis()
    if (current - lastSent >= 1000) {
      lastSent = current
      java.util.List.of(RowSourceRecord.builder().topicKey(topicKey).row(MyConnectorTask.ROW).build())
    } else java.util.List.of()
  }
}

object MyConnectorTask {
  val ROW: Row = Row.of(Cell.of("f0", 13), Cell.of("f1", false))
}
