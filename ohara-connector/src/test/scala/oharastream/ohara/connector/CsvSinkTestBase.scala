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

package oharastream.ohara.connector

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data._
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions._
import oharastream.ohara.kafka.connector.csv.CsvSinkConnector
import oharastream.ohara.kafka.Producer
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}

abstract class CsvSinkTestBase extends With3Brokers3Workers {
  protected val fileSystem: FileSystem
  protected val connectorClass: Class[_ <: CsvSinkConnector]
  protected def setupProps: Map[String, String]

  private[this] val defaultProps: Map[String, String] = Map(
    OUTPUT_FOLDER_KEY      -> "/output",
    FLUSH_SIZE_KEY         -> "3",
    ROTATE_INTERVAL_MS_KEY -> "0", // don't auto commit on time
    FILE_NEED_HEADER_KEY   -> "false",
    FILE_ENCODE_KEY        -> "UTF-8"
  )

  private[this] def props: Map[String, String] = defaultProps ++ setupProps

  private[this] def outputFolder = props(OUTPUT_FOLDER_KEY)

  private[this] val schema: Seq[Column] = Seq(
    Column.builder().name("a").dataType(DataType.STRING).order(1).build(),
    Column.builder().name("b").dataType(DataType.INT).order(2).build(),
    Column.builder().name("c").dataType(DataType.BOOLEAN).order(3).build()
  )

  private[this] val row = Row.of(Cell.of("a", "abc"), Cell.of("b", 123), Cell.of("c", true))

  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] def pushData(data: Seq[Row], topicKey: TopicKey): Unit = {
    val producer = Producer
      .builder()
      .connectionProps(testUtil.brokersConnProps)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()
    try {
      data.foreach(d => producer.sender().topicKey(topicKey).key(d).send().get())
    } finally producer.close()
  }

  private[this] def fetchData(topicKey: TopicKey): Seq[String] = {
    val dir = Paths.get(outputFolder, topicKey.topicNameOnKafka(), "partition0").toString
    if (fileSystem.exists(dir)) {
      listCommittedFileNames(dir)
        .map(fileName => Paths.get(dir, fileName).toString)
        .flatMap(filePath => fileSystem.readLines(filePath))
    } else Seq()
  }

  private[this] def listCommittedFileNames(dir: String): Seq[String] =
    fileSystem.listFileNames(dir, (fileName: String) => !fileName.contains("_tmp"))

  private[this] def setupConnector(props: Map[String, String], schema: Seq[Column]): TopicKey =
    setupConnector(props, Some(schema))

  private[this] def setupConnector(props: Map[String, String], schema: Option[Seq[Column]]): TopicKey = {
    // create a connector and check its state is running
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result({
      val creator = connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(connectorClass)
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .settings(props)
      if (schema.isDefined) creator.columns(schema.get)
      creator.create()
    })
    ConnectorTestUtils.checkConnector(testUtil, connectorKey)
    topicKey
  }

  private[this] def createReplicaData[T](data: T, number: Int): Seq[T] = 0 until number map (_ => data)

  @BeforeEach
  def setup(): Unit = {
    fileSystem.reMkdirs(outputFolder)
    fileSystem.exists(outputFolder) shouldBe true
    fileSystem.listFileNames(outputFolder).asScala.size shouldBe 0
  }

  @Test
  def testNormalCase(): Unit = {
    val topicKey = setupConnector(props, schema)
    val data     = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).lengthCompare(3) == 0, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe 3

    receivedData.foreach { line =>
      line shouldBe row.cells.asScala.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testReorder(): Unit = {
    val newSchema: Seq[Column] = Seq(
      Column.builder().name("a").dataType(DataType.STRING).order(3).build(),
      Column.builder().name("b").dataType(DataType.INT).order(2).build(),
      Column.builder().name("c").dataType(DataType.BOOLEAN).order(1).build()
    )
    val topicKey = setupConnector(props, newSchema)
    val data     = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 3, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size

    receivedData.foreach { line =>
      line shouldBe row.cells.asScala.reverse.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testHeader(): Unit = {
    // need header
    val newProps = props ++ Map(FILE_NEED_HEADER_KEY -> "true")
    val topicKey = setupConnector(newProps, schema)
    val data     = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 4, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size + 1
    receivedData.head shouldBe schema.sortBy(_.order).map(_.name).mkString(",")
  }

  @Test
  def testHeaderWithoutSchema(): Unit = {
    // need header
    val newProps = props ++ Map(FILE_NEED_HEADER_KEY -> "true")
    // without schema
    val topicKey = setupConnector(newProps, None)
    val data     = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 4, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size + 1
    receivedData.head shouldBe schema.sortBy(_.order).map(_.name).mkString(",")
    receivedData.drop(1).foreach { line =>
      line shouldBe row.cells.asScala.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testColumnRename(): Unit = {
    // need header
    val newProps = props ++ Map(FILE_NEED_HEADER_KEY -> "true")
    val newSchema = Seq(
      Column.builder().name("a").newName("aa").dataType(DataType.STRING).order(1).build(),
      Column.builder().name("b").newName("bb").dataType(DataType.INT).order(2).build(),
      Column.builder().name("c").newName("cc").dataType(DataType.BOOLEAN).order(3).build()
    )
    val topicKey = setupConnector(newProps, newSchema)
    val data     = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 4, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size + 1
    receivedData.head shouldBe newSchema.sortBy(_.order).map(_.newName).mkString(",")
    receivedData.drop(1).foreach { line =>
      line shouldBe row.cells.asScala.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testWithoutEncode(): Unit = {
    // will use default UTF-8
    val newProps = props - FILE_ENCODE_KEY
    val topicKey = setupConnector(newProps, schema)
    val data     = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 3, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size
    receivedData.foreach { line =>
      line shouldBe row.cells.asScala.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testPartialColumns(): Unit = {
    // skip last column
    val newSchema = schema.slice(0, schema.length - 1)
    val topicKey  = setupConnector(props, newSchema)
    val data      = createReplicaData(row, 3)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 3, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size

    val items = receivedData.head.split(",")
    items.length shouldBe row.size - 1

    items(0) shouldBe row.cell(0).value.toString
    items(1) shouldBe row.cell(1).value.toString
  }

  @Test
  def testUnmatchedSchema(): Unit = {
    // the name can't be casted to int
    val newSchema = Seq(Column.builder().name("name").dataType(DataType.INT).order(1).build())
    val topicKey  = setupConnector(props, newSchema)
    val data      = createReplicaData(row, 3)
    pushData(data, topicKey)
    TimeUnit.SECONDS.sleep(5)
    fetchData(topicKey).size shouldBe 0
  }

  @Test
  def testCommitPer10Records(): Unit = {
    // auto commit per 10 records
    val newProps = props ++ Map(FLUSH_SIZE_KEY -> "10")
    val topicKey = setupConnector(newProps, schema)
    val data     = createReplicaData(row, 10)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 10, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size
    receivedData.foreach { line =>
      line shouldBe row.cells.asScala.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testCommitPer10Seconds(): Unit = {
    // auto commit per Duration(20, TimeUnit.SECONDS)
    val newProps = props ++ Map(
      FLUSH_SIZE_KEY         -> Int.MaxValue.toString, // don't commit by size
      ROTATE_INTERVAL_MS_KEY -> "10000"
    )
    val topicKey = setupConnector(newProps, schema)
    val data     = createReplicaData(row, 99)
    pushData(data, topicKey)
    // connector is running in async mode so we have to wait data is pushed to connector
    CommonUtils.await(() => fetchData(topicKey).size == 99, java.time.Duration.ofSeconds(20))
    val receivedData = fetchData(topicKey)
    receivedData.size shouldBe data.size
    receivedData.foreach { line =>
      line shouldBe row.cells.asScala.map(cell => cell.value).mkString(",")
    }
  }

  @Test
  def testNonMappingSchema(): Unit = {
    val newSchema = Seq(Column.builder().name("d").dataType(DataType.BOOLEAN).order(1).build())
    val topicKey  = setupConnector(props, newSchema)
    val data      = createReplicaData(row, 3)
    pushData(data, topicKey)
    TimeUnit.SECONDS.sleep(5)
    fetchData(topicKey).size shouldBe 0
  }

  @Test
  def testNonexistentInputFolder(): Unit =
    ConnectorTestUtils.nonexistentFolderShouldFail(fileSystem, connectorClass, props, props(OUTPUT_FOLDER_KEY))

  @Test
  def testFileToInputFolder(): Unit =
    ConnectorTestUtils.fileShouldFail(fileSystem, connectorClass, props, props(OUTPUT_FOLDER_KEY))

  @AfterEach
  def tearDown(): Unit = Releasable.close(fileSystem)
}
