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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.ConnectorApi.State
import oharastream.ohara.common.data._
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.{Consumer, Producer, TopicAdmin}
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
class TestDataTransmissionOnCluster extends WithBrokerWorker {
  private[this] val topicAdmin     = TopicAdmin.of(testUtil().brokersConnProps)
  private[this] val connectorAdmin = ConnectorAdmin(testUtil().workersConnProps())
  private[this] val row            = Row.of(Cell.of("cf0", 10), Cell.of("cf1", 11))
  private[this] val schema         = Seq(Column.builder().name("cf").dataType(DataType.BOOLEAN).order(1).build())
  private[this] val numberOfRows   = 20

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(60, TimeUnit.SECONDS))

  private[this] def await(f: () => Boolean): Unit = CommonUtils.await(() => f(), java.time.Duration.ofSeconds(300))

  @AfterEach
  def tearDown(): Unit = Releasable.close(topicAdmin)

  private[this] def createTopic(topicKey: TopicKey, compacted: Boolean): Unit = {
    if (compacted)
      topicAdmin
        .topicCreator()
        .compacted()
        .numberOfPartitions(1)
        .numberOfReplications(1)
        .topicKey(topicKey)
        .create()
    else
      topicAdmin
        .topicCreator()
        .deleted()
        .numberOfPartitions(1)
        .numberOfReplications(1)
        .topicKey(topicKey)
        .create()
  }

  private[this] def setupData(topicKey: TopicKey): Unit = {
    val producer = Producer.builder
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try 0 until numberOfRows foreach (_ => producer.sender().key(row).topicKey(topicKey).send())
    finally producer.close()
    checkData(topicKey)
  }

  private[this] def checkData(topicKey: TopicKey): Unit = {
    val consumer = Consumer
      .builder()
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .topicKey(topicKey)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    val data = consumer.poll(java.time.Duration.ofSeconds(30), numberOfRows)
    data.size shouldBe numberOfRows
    data.asScala.foreach(_.key.get shouldBe row)
  }

  private[this] def checkConnector(connectorKey: ConnectorKey): Unit = {
    await(() => result(connectorAdmin.activeConnectors()).contains(connectorKey))
    await(() => result(connectorAdmin.config(connectorKey)).topicNames.nonEmpty)
    await(
      () =>
        try result(connectorAdmin.status(connectorKey)).connector.state == State.RUNNING.name
        catch {
          case _: Throwable => false
        }
    )
  }

  @Test
  def testRowProducer2RowConsumer(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    //test deleted topic
    createTopic(topicKey, false)
    testRowProducer2RowConsumer(topicKey)

    val topicKey2 = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    //test compacted topic
    createTopic(topicKey2, true)
    testRowProducer2RowConsumer(topicKey2)
  }

  /**
    * producer -> topic_1(topicName) -> consumer
    */
  private[this] def testRowProducer2RowConsumer(topicKey: TopicKey): Unit = {
    setupData(topicKey)
    val consumer = Consumer
      .builder()
      .offsetFromBegin()
      .connectionProps(testUtil.brokersConnProps)
      .topicKey(topicKey)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try {
      val data = consumer.poll(java.time.Duration.ofSeconds(30), numberOfRows)
      data.size shouldBe numberOfRows
      data.asScala.foreach(_.key.get shouldBe row)
    } finally consumer.close()
  }

  @Test
  def testProducer2SinkConnector(): Unit = {
    val srcKey    = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val targetKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    //test deleted topic
    createTopic(srcKey, false)
    createTopic(targetKey, false)
    testProducer2SinkConnector(srcKey, targetKey)

    val srcKey2    = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val targetKey2 = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    //test compacted topic
    createTopic(srcKey2, true)
    createTopic(targetKey2, true)
    testProducer2SinkConnector(srcKey2, targetKey2)
  }

  /**
    * producer -> topic_1(topicName) -> sink connector -> topic_2(topicName2)
    */
  private[this] def testProducer2SinkConnector(srcKey: TopicKey, targetKey: TopicKey): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[SimpleRowSinkConnector])
        .topicKey(srcKey)
        .numberOfTasks(1)
        .columns(schema)
        .settings(
          Map(
            SimpleRowSinkConnector.BROKER -> testUtil.brokersConnProps,
            SimpleRowSinkConnector.OUTPUT -> TopicKey.toJsonString(java.util.List.of(targetKey))
          )
        )
        .create()
    )

    try {
      checkConnector(connectorKey)
      setupData(srcKey)
      checkData(targetKey)
    } finally result(connectorAdmin.delete(connectorKey))
  }

  @Test
  def testSourceConnector2Consumer(): Unit = {
    val srcKey    = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val targetKey = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    //test deleted topic
    createTopic(srcKey, false)
    createTopic(targetKey, false)
    testSourceConnector2Consumer(srcKey, targetKey)

    val srcKey2    = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val targetKey2 = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    //test compacted topic
    createTopic(srcKey2, true)
    createTopic(targetKey2, true)
    testSourceConnector2Consumer(srcKey2, targetKey2)
  }

  /**
    * producer -> topic_1(topicName) -> row source -> topic_2 -> consumer
    */
  private[this] def testSourceConnector2Consumer(srcKey: TopicKey, targetKey: TopicKey): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[SimpleRowSourceConnector])
        .topicKey(targetKey)
        .numberOfTasks(1)
        .columns(schema)
        .settings(
          Map(
            SimpleRowSourceConnector.BROKER -> testUtil.brokersConnProps,
            SimpleRowSourceConnector.INPUT  -> TopicKey.toJsonString(java.util.List.of(srcKey))
          )
        )
        .create()
    )

    try {
      checkConnector(connectorKey)
      setupData(srcKey)
      checkData(targetKey)
    } finally result(connectorAdmin.delete(connectorKey))
  }

  /**
    * Test case for OHARA-150
    */
  @Test
  def shouldKeepColumnOrderAfterSendToKafka(): Unit = {
    val topicKey   = TopicKey.of("g", CommonUtils.randomString(10))
    val topicAdmin = TopicAdmin.of(testUtil().brokersConnProps())
    try topicAdmin
      .topicCreator()
      .topicKey(topicKey)
      .numberOfPartitions(1)
      .numberOfReplications(1)
      .create()
    finally topicAdmin.close()

    val row = Row.of(Cell.of("c", 3), Cell.of("b", 2), Cell.of("a", 1))
    val producer = Producer.builder
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try {
      producer.sender().key(row).topicKey(topicKey).send()
      producer.flush()
    } finally producer.close()

    val consumer =
      Consumer
        .builder()
        .connectionProps(testUtil.brokersConnProps)
        .offsetFromBegin()
        .topicKey(topicKey)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()

    try {
      val fromKafka = consumer.poll(java.time.Duration.ofSeconds(5), 1).asScala
      fromKafka.isEmpty shouldBe false
      val row = fromKafka.head.key.get
      row.cell(0).name shouldBe "c"
      row.cell(1).name shouldBe "b"
      row.cell(2).name shouldBe "a"
    } finally consumer.close()
  }

  /**
    * Test for ConnectorClient
    * @see ConnectorClient
    */
  @Test
  def testConnectorAdmin(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    val topicKeys    = Set(TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10)))
    val outputTopic  = TopicKey.of(CommonUtils.randomString(10), CommonUtils.randomString(10))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[SimpleRowSinkConnector])
        .topicKeys(topicKeys)
        .numberOfTasks(1)
        .columns(schema)
        .settings(
          Map(
            SimpleRowSinkConnector.BROKER -> testUtil.brokersConnProps,
            SimpleRowSinkConnector.OUTPUT -> TopicKey.toJsonString(java.util.List.of(outputTopic))
          )
        )
        .create()
    )

    val activeConnectors = result(connectorAdmin.activeConnectors())
    activeConnectors.contains(connectorKey) shouldBe true

    val config = result(connectorAdmin.config(connectorKey))
    config.topicNames shouldBe topicKeys.map(_.topicNameOnKafka)

    await(
      () =>
        try result(connectorAdmin.status(connectorKey)).tasks.nonEmpty
        catch {
          case _: Throwable => false
        }
    )
    val status = result(connectorAdmin.status(connectorKey))
    status.tasks.head should not be null

    val task = result(connectorAdmin.taskStatus(connectorKey, status.tasks.head.id))
    task should not be null
    task == status.tasks.head shouldBe true
    task.worker_id.isEmpty shouldBe false

    result(connectorAdmin.delete(connectorKey))
    result(connectorAdmin.activeConnectors()).contains(connectorKey) shouldBe false
  }
}
