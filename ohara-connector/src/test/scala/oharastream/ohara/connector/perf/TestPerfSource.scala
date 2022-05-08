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

package oharastream.ohara.connector.perf
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data._
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.connector.ConnectorTestUtils
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestPerfSource extends With3Brokers3Workers {
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  private[this] val props = PerfSourceProps(
    batch = 5,
    freq = Duration(5, TimeUnit.SECONDS),
    cellSize = 1
  )

  private[this] val schema: Seq[Column] =
    Seq(
      Column.builder().name("a").dataType(DataType.STRING).order(1).build(),
      Column.builder().name("b").dataType(DataType.SHORT).order(2).build(),
      Column.builder().name("c").dataType(DataType.INT).order(3).build(),
      Column.builder().name("d").dataType(DataType.LONG).order(4).build(),
      Column.builder().name("e").dataType(DataType.FLOAT).order(5).build(),
      Column.builder().name("f").dataType(DataType.DOUBLE).order(6).build(),
      Column.builder().name("g").dataType(DataType.BOOLEAN).order(7).build(),
      Column.builder().name("h").dataType(DataType.BYTE).order(8).build(),
      Column.builder().name("i").dataType(DataType.BYTES).order(9).build()
    )

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(10, TimeUnit.SECONDS))

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

  private[this] def matchType(lhs: Class[_], dataType: DataType): Unit = {
    dataType match {
      case DataType.STRING  => lhs shouldBe classOf[java.lang.String]
      case DataType.SHORT   => lhs shouldBe classOf[java.lang.Short]
      case DataType.INT     => lhs shouldBe classOf[java.lang.Integer]
      case DataType.LONG    => lhs shouldBe classOf[java.lang.Long]
      case DataType.FLOAT   => lhs shouldBe classOf[java.lang.Float]
      case DataType.DOUBLE  => lhs shouldBe classOf[java.lang.Double]
      case DataType.BOOLEAN => lhs shouldBe classOf[java.lang.Boolean]
      case DataType.BYTE    => lhs shouldBe classOf[java.lang.Byte]
      case DataType.BYTES   => lhs shouldBe classOf[Array[java.lang.Byte]]
      case _                => throw new IllegalArgumentException("unsupported type in testing TestPerfSource")
    }
  }

  @Test
  def testNormalCase(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[PerfSource])
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .columns(schema)
        .settings(props.toMap)
        .create()
    )

    try {
      ConnectorTestUtils.checkConnector(testUtil, connectorKey)
      // it is hard to evaluate number from records in topics so we just fetch some records here.
      val records = pollData(topicKey, props.freq * 3, props.batch)
      records.size >= props.batch shouldBe true
      records
        .map(_.key.get)
        .foreach(row => {
          row.size shouldBe schema.size
          schema.foreach(c => {
            val cell: Cell[_] = row.cell(c.order - 1)
            cell.name shouldBe c.name
            matchType(cell.value.getClass, c.dataType)
          })
        })
    } finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testNormalCaseWithoutBatch(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[PerfSource])
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .columns(schema)
        .settings(Map(PerfSourceProps.PERF_FREQUENCY_KEY -> "PT5S"))
        .create()
    )

    try {
      ConnectorTestUtils.checkConnector(testUtil, connectorKey)
      // it is hard to evaluate number from records in topics so we just fetch some records here.
      val records = pollData(topicKey, props.freq * 3, props.batch)
      records.size >= props.batch shouldBe true
      records
        .map(_.key.get)
        .foreach(row => {
          row.size shouldBe schema.size
          schema.foreach(c => {
            val cell: Cell[_] = row.cell(c.order - 1)
            cell.name shouldBe c.name
            matchType(cell.value.getClass, c.dataType)
          })
        })
    } finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testNormalCaseWithoutFrequence(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[PerfSource])
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .columns(schema)
        .settings(Map(PerfSourceProps.PERF_BATCH_KEY -> "5"))
        .create()
    )

    try {
      ConnectorTestUtils.checkConnector(testUtil, connectorKey)
      // it is hard to evaluate number from records in topics so we just fetch some records here.
      val records = pollData(topicKey, props.freq * 3, props.batch)
      records.size >= props.batch shouldBe true
      records
        .map(_.key.get)
        .foreach(row => {
          row.size shouldBe schema.size
          schema.foreach(c => {
            val cell: Cell[_] = row.cell(c.order - 1)
            cell.name shouldBe c.name
            matchType(cell.value.getClass, c.dataType)
          })
        })
    } finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testNormalCaseWithoutInput(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[PerfSource])
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .columns(schema)
        .settings(Map.empty)
        .create()
    )

    try {
      ConnectorTestUtils.checkConnector(testUtil, connectorKey)
      // it is hard to evaluate number from records in topics so we just fetch some records here.
      val records = pollData(topicKey, props.freq * 3, props.batch)
      records.size >= props.batch shouldBe true
      records
        .map(_.key.get)
        .foreach(row => {
          row.size shouldBe schema.size
          schema.foreach(c => {
            val cell: Cell[_] = row.cell(c.order - 1)
            cell.name shouldBe c.name
            matchType(cell.value.getClass, c.dataType)
          })
        })
    } finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testInvalidInput(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    an[IllegalArgumentException] should be thrownBy result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[PerfSource])
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .columns(schema)
        .settings(Map(PerfSourceProps.PERF_FREQUENCY_KEY -> "abcd"))
        .create()
    )
  }

  @Test
  def testInvalidInputWithNegative(): Unit = {
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[PerfSource])
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .columns(schema)
        .settings(Map(PerfSourceProps.PERF_BATCH_KEY -> "-1"))
        .create()
    )
    ConnectorTestUtils.assertFailedConnector(testUtil, connectorKey)
  }
}
