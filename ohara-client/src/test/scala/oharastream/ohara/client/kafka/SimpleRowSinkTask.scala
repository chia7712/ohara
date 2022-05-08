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

import oharastream.ohara.common.data.{Row, Serializer}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.Producer
import oharastream.ohara.kafka.connector.{RowSinkRecord, RowSinkTask, TaskSetting}

import scala.jdk.CollectionConverters._

class SimpleRowSinkTask extends RowSinkTask {
  private[this] var outputTopics: Set[TopicKey]          = _
  private[this] var producer: Producer[Row, Array[Byte]] = _
  override protected def run(settings: TaskSetting): Unit = {
    outputTopics = TopicKey.toTopicKeys(settings.stringValue(SimpleRowSinkConnector.OUTPUT)).asScala.toSet
    producer = Producer.builder
      .connectionProps(settings.stringValue(SimpleRowSinkConnector.BROKER))
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
  }

  override protected def terminate(): Unit = Releasable.close(producer)

  override protected def putRecords(records: util.List[RowSinkRecord]): Unit =
    outputTopics.foreach(
      outputTopic =>
        records.asScala
          .foreach(r => producer.sender().key(r.row()).topicKey(outputTopic).send())
    )
}
