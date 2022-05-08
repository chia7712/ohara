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

package oharastream.ohara.connector.ftp

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Cell, Column, DataType, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class TestFtpSourceConnector extends With3Brokers3Workers {
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  private[this] val schema: Seq[Column] = Seq(
    Column.builder().name("index").dataType(DataType.INT).build(),
    Column.builder().name("name").dataType(DataType.STRING).order(1).build(),
    Column.builder().name("ranking").dataType(DataType.INT).order(2).build(),
    Column.builder().name("single").dataType(DataType.BOOLEAN).order(3).build()
  )

  private[this] val sourceFileNumber = 8
  private[this] val oneFileDataCount = 1000
  private[this] val fileSystem = FileSystem.ftpBuilder
    .hostname(testUtil.ftpServer.hostname)
    .port(testUtil.ftpServer.port)
    .user(testUtil.ftpServer.user)
    .password(testUtil.ftpServer.password)
    .build()

  private[this] val inputFolder     = "/input"
  private[this] val completedFolder = "/complete"
  private[this] val outputFolder    = "/output"

  @BeforeEach
  def setup(): Unit = {
    createFolder(fileSystem, inputFolder)
    createFolder(fileSystem, completedFolder)
    createFolder(fileSystem, outputFolder)
    (1 to sourceFileNumber).foreach { i =>
      val row            = Row.of(Cell.of("index", 0), Cell.of("name", "chia"), Cell.of("ranking", 1), Cell.of("single", false))
      val header: String = row.cells().asScala.map(_.name).mkString(",")
      val data = ((oneFileDataCount * i) - oneFileDataCount + 1 to (oneFileDataCount * i))
        .map(
          j =>
            row
              .cells()
              .asScala
              .zipWithIndex
              .map {
                case (cell, index) =>
                  if (index == 0) Cell.of("index", j)
                  else cell
              }
              .map(_.value.toString)
              .mkString(",")
        )
      setupInput(fileSystem, inputFolder, header, data)
    }
  }

  @Test
  def testListFileIntoQueue(): Unit = {
    val topicKey           = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val sourceConnectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    try {
      // start ftp source connector
      Await.result(
        connectorAdmin
          .connectorCreator()
          .topicKey(topicKey)
          .connectorClass(classOf[FtpSource])
          .numberOfTasks(1)
          .connectorKey(sourceConnectorKey)
          .columns(schema)
          .settings(
            Map(
              INPUT_FOLDER_KEY     -> inputFolder,
              COMPLETED_FOLDER_KEY -> completedFolder,
              FTP_HOSTNAME_KEY     -> testUtil.ftpServer.hostname(),
              FTP_PORT_KEY         -> testUtil.ftpServer.port().toString,
              FTP_USER_NAME_KEY    -> testUtil.ftpServer.user(),
              FTP_PASSWORD_KEY     -> testUtil.ftpServer.password,
              FILE_CACHE_SIZE_KEY  -> "3"
            )
          )
          .create(),
        Duration(10, TimeUnit.SECONDS)
      )

      val allFileRecordCount = sourceFileNumber * oneFileDataCount
      val records            = pollData(topicKey, Duration(60, TimeUnit.SECONDS), allFileRecordCount)
      records.size shouldBe allFileRecordCount

      records
        .map(
          record =>
            record
              .key()
              .get()
              .cell(0)
              .value()
              .asInstanceOf[Int]
        )
        .sorted
        .zipWithIndex
        .map {
          case (result, index) => {
            result shouldBe (index + 1)
          }
        }
    } finally connectorAdmin.delete(sourceConnectorKey)
  }

  private[this] def setupInput(fileSystem: FileSystem, inputFolder: String, header: String, data: Seq[String]): Unit = {
    val writer = new BufferedWriter(
      new OutputStreamWriter(
        fileSystem.create(
          oharastream.ohara.common.util.CommonUtils.path(inputFolder, s"${CommonUtils.randomString(8)}.csv")
        )
      )
    )
    try {
      writer.append(header)
      writer.newLine()
      data.foreach(line => {
        writer.append(line)
        writer.newLine()
      })
    } finally writer.close()
  }

  private[this] def createFolder(fileSystem: FileSystem, path: String): Unit = {
    if (fileSystem.exists(path)) {
      fileSystem
        .listFileNames(path)
        .asScala
        .map(oharastream.ohara.common.util.CommonUtils.path(path, _))
        .foreach(fileSystem.delete)
      fileSystem.listFileNames(path).asScala.size shouldBe 0
      fileSystem.delete(path)
    }
    fileSystem.mkdirs(path)
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

  @AfterEach
  def tearDown(): Unit = Releasable.close(fileSystem)
}
