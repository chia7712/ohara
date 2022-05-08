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

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.exception.NoSuchFileException
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import oharastream.ohara.kafka.connector.json.ConnectorFormatter
import oharastream.ohara.testing.service.FtpServer
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestFtpSourceTask extends OharaTest {
  private[this] val ftpServer = FtpServer.builder().controlPort(0).dataPorts(java.util.Arrays.asList(0, 0, 0)).build()

  private[this] val inputFolder     = "/input"
  private[this] val completedFolder = "/backup"
  private[this] val errorFolder     = "/error"

  private[this] val props = FtpSourceProps(
    user = ftpServer.user,
    password = ftpServer.password,
    hostname = ftpServer.hostname,
    port = ftpServer.port
  )

  private[this] val settings = props.toMap ++ Map(
    CsvConnectorDefinitions.INPUT_FOLDER_KEY     -> inputFolder,
    CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> completedFolder,
    CsvConnectorDefinitions.ERROR_FOLDER_KEY     -> errorFolder,
    CsvConnectorDefinitions.TASK_TOTAL_KEY       -> "1",
    CsvConnectorDefinitions.TASK_HASH_KEY        -> "0"
  )

  @BeforeEach
  def setup(): Unit = {
    val fileSystem = createFileSystem()

    try {
      fileSystem.reMkdirs(inputFolder)
      fileSystem.reMkdirs(completedFolder)
      fileSystem.reMkdirs(errorFolder)
    } finally fileSystem.close()
  }

  private[this] def createFileSystem(): FileSystem = {
    val task   = createTask()
    val config = TaskSetting.of(settings.asJava)
    task.fileSystem(config).asInstanceOf[FileSystem]
  }

  private[this] def createTask() = {
    val task = new FtpSourceTask()
    task.start(
      ConnectorFormatter
        .of()
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .topicKey(TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .settings(settings.asJava)
        .raw()
    )
    task
  }

  @Test
  def testFileSystem(): Unit = {
    val task   = createTask()
    val config = TaskSetting.of(settings.asJava)
    task.fileSystem(config) should not be (null)
  }

  @Test
  def testFileSystem_WithEmptyConfig(): Unit = {
    val task = createTask()
    intercept[NoSuchElementException] {
      task.fileSystem(TaskSetting.of(java.util.Map.of()))
    }
  }

  @Test
  def testListNonexistentInput(): Unit = {
    val fileSystem = createFileSystem()
    try fileSystem.delete(inputFolder)
    finally fileSystem.close()

    val fs = createFileSystem()
    // input folder doesn't exist should throw error
    intercept[NoSuchFileException] {
      fs.listFileNames(inputFolder).asScala.size shouldBe 0
    }
  }

  @Test
  def testListInput(): Unit = {
    val numberOfInputs = 3
    val fileSystem     = createFileSystem()
    try {
      val data = (0 to 100).map(_.toString)
      (0 until numberOfInputs).foreach(
        index => fileSystem.attach(CommonUtils.path(inputFolder, index.toString), data)
      )
    } finally fileSystem.close()

    val fs = createFileSystem()
    fs.listFileNames(inputFolder).asScala.size shouldBe 3
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(ftpServer)
}
