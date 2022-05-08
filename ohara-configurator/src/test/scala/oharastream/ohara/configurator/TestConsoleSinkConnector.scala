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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.ConnectorApi.State
import oharastream.ohara.client.configurator.TopicApi.TopicInfo
import oharastream.ohara.client.configurator.{BrokerApi, ConnectorApi, TopicApi, WorkerApi}
import oharastream.ohara.common.data.{Cell, Column, DataType, Row, Serializer}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.console.ConsoleSink
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.{Consumer, Producer}
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import org.scalatest.matchers.should.Matchers._

class TestConsoleSinkConnector extends WithBrokerWorker {
  private[this] val configurator =
    Configurator.builder.fake(testUtil.brokersConnProps, testUtil().workersConnProps()).build()
  private[this] val connectorApi = ConnectorApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val topicApi = TopicApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(10, TimeUnit.SECONDS))

  private[this] def await(f: () => Boolean): Unit = CommonUtils.await(() => f(), java.time.Duration.ofSeconds(40))

  private[this] val workerClusterInfo = result(
    WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head
  private[this] val rowDataTotalSize = 100
  private[this] var topic: TopicInfo = _

  @BeforeEach
  def setup(): Unit = {
    val producer = Producer
      .builder()
      .keySerializer(Serializer.ROW)
      .connectionProps(testUtil.brokersConnProps)
      .build()
    try {
      topic = result(
        topicApi.request
          .name(CommonUtils.randomString(10))
          .brokerClusterKey(
            result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
          )
          .create()
      )
      result(topicApi.start(topic.key))

      rowData().foreach { row =>
        producer
          .sender()
          .topicKey(topic.key)
          .key(row)
          .send()
      }
    } finally Releasable.close(producer)
  }

  @Test
  def testTypeError(): Unit = {
    val columns = Seq(
      Column.builder.name("c1").newName("column1").dataType(DataType.INT).order(0).build(),
      Column.builder.name("c2").newName("column2").dataType(DataType.INT).order(1).build(),
      Column.builder.name("c3").newName("column3").dataType(DataType.BOOLEAN).order(2).build()
    )
    val sink = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[ConsoleSink].getName)
        .numberOfTasks(1)
        .topicKey(topic.key)
        .columns(columns)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    (0 until 3).foreach(_ => result(connectorApi.start(sink.key)))
    await(() => result(connectorApi.get(sink.key)).state.contains(State.RUNNING))
    val records = pollData(topic.key, Duration(60, TimeUnit.SECONDS), rowDataTotalSize)
    records.size shouldBe rowDataTotalSize // Confirm the topic have data
    await(() => result(connectorApi.get(sink.key)).error.nonEmpty)
    await(() => result(connectorApi.get(sink.key)).error.get.contains("expected type: INT, actual:java.lang.String"))
  }

  private[this] def rowData(): Seq[Row] =
    (0 until rowDataTotalSize).map { _ =>
      Row.of(
        Cell.of("c1", CommonUtils.randomString(5)),
        Cell.of("c2", 100),
        Cell.of("c3", true)
      )
    }
  private[this] def pollData(
    topicKey: TopicKey,
    timeout: scala.concurrent.duration.Duration,
    size: Int
  ): Seq[Record[Row, Array[Byte]]] = {
    val consumer = Consumer
      .builder()
      .topicKey(topicKey)
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try consumer.poll(java.time.Duration.ofNanos(timeout.toNanos), size).asScala.toSeq
    finally consumer.close()
  }
}
