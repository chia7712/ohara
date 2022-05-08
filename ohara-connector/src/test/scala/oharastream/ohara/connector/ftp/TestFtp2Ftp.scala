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
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.With3Brokers3Workers
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
  * ftp csv -> topic -> ftp csv
  */
class TestFtp2Ftp extends With3Brokers3Workers {
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  private[this] val schema: Seq[Column] = Seq(
    Column.builder().name("name").dataType(DataType.STRING).order(1).build(),
    Column.builder().name("ranking").dataType(DataType.INT).order(2).build(),
    Column.builder().name("single").dataType(DataType.BOOLEAN).order(3).build()
  )

  private[this] val row            = Row.of(Cell.of("name", "chia"), Cell.of("ranking", 1), Cell.of("single", false))
  private[this] val header: String = row.cells().asScala.map(_.name).mkString(",")
  private[this] val data           = (1 to 1000).map(_ => row.cells().asScala.map(_.value.toString).mkString(","))

  private[this] val fileSystem = FileSystem.ftpBuilder
    .hostname(testUtil.ftpServer.hostname)
    .port(testUtil.ftpServer.port)
    .user(testUtil.ftpServer.user)
    .password(testUtil.ftpServer.password)
    .build()

  private[this] val inputFolder     = "/input"
  private[this] val completedFolder = "/backup"
  private[this] val outputFolder    = "/output"

  @BeforeEach
  def setup(): Unit = {
    rebuild(fileSystem, inputFolder)
    rebuild(fileSystem, completedFolder)
    rebuild(fileSystem, outputFolder)
    setupInput(fileSystem, inputFolder, header, data)
  }

  @Test
  def testNormalCase(): Unit = {
    val topicKey           = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val sinkConnectorKey   = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val sourceConnectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    // start sink
    Await.result(
      connectorAdmin
        .connectorCreator()
        .topicKey(topicKey)
        .connectorClass(classOf[FtpSink])
        .numberOfTasks(1)
        .connectorKey(sinkConnectorKey)
        .columns(schema)
        .settings(
          Map(
            OUTPUT_FOLDER_KEY -> outputFolder,
            FTP_HOSTNAME_KEY  -> testUtil.ftpServer.hostname(),
            FTP_PORT_KEY      -> testUtil.ftpServer.port().toString,
            FTP_USER_NAME_KEY -> testUtil.ftpServer.user(),
            FTP_PASSWORD_KEY  -> testUtil.ftpServer.password
          )
        )
        .create(),
      Duration(20, TimeUnit.SECONDS)
    )

    try {
      try {
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
                FTP_PASSWORD_KEY     -> testUtil.ftpServer.password
              )
            )
            .create(),
          Duration(20, TimeUnit.SECONDS)
        )
        CommonUtils
          .await(() => fileSystem.listFileNames(inputFolder).asScala.isEmpty, java.time.Duration.ofSeconds(30))
        CommonUtils.await(
          () => fileSystem.listFileNames(completedFolder).asScala.size == 1,
          java.time.Duration.ofSeconds(30)
        )
        val committedFolder = CommonUtils.path(outputFolder, topicKey.topicNameOnKafka(), "partition0")
        CommonUtils.await(() => {
          if (fileSystem.exists(committedFolder))
            listCommittedFiles(committedFolder).size == 1
          else false
        }, java.time.Duration.ofSeconds(30))
        val lines =
          fileSystem.readLines(
            oharastream.ohara.common.util.CommonUtils
              .path(committedFolder, fileSystem.listFileNames(committedFolder).asScala.toSeq.head)
          )
        lines.length shouldBe data.length + 1 // header
        lines(0) shouldBe header
        lines(1) shouldBe data.head
        lines(2) shouldBe data(1)
      } finally connectorAdmin.delete(sourceConnectorKey)
    } finally connectorAdmin.delete(sinkConnectorKey)
  }

  private[this] def listCommittedFiles(dir: String): Seq[String] = {
    fileSystem.listFileNames(dir, (fileName: String) => !fileName.contains("_tmp"))
  }

  /**
    * delete all stuffs in the path and then recreate it as a folder
    * @param fileSystem ftp client
    * @param path path on ftp server
    */
  private[this] def rebuild(fileSystem: FileSystem, path: String): Unit = {
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

  private[this] def setupInput(fileSystem: FileSystem, inputFolder: String, header: String, data: Seq[String]): Unit = {
    val writer = new BufferedWriter(
      new OutputStreamWriter(
        fileSystem.create(oharastream.ohara.common.util.CommonUtils.path(inputFolder, "abc"))
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

  @AfterEach
  def tearDown(): Unit = Releasable.close(fileSystem)
}
