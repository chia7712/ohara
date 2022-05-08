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

package oharastream.ohara.connector.hdfs.sink

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.setting.SettingDef.{Necessary, Permission, Reference}
import oharastream.ohara.common.setting.{ConnectorKey, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class TestHDFSSinkDefinition extends WithBrokerWorker {
  private[this] val hdfsSink                   = new HDFSSink
  private[this] val connectorAdmin             = ConnectorAdmin(testUtil().workersConnProps())
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(10, TimeUnit.SECONDS))

  @Test
  def checkHdfsURL(): Unit = {
    val definition = hdfsSink.settingDefinitions().get(HDFS_URL_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkTopicsDir(): Unit = {
    val definition = hdfsSink.settingDefinitions().get(OUTPUT_FOLDER_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkFlushSize(): Unit = {
    val definition = hdfsSink.settingDefinitions().get(FLUSH_SIZE_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultInt() shouldBe FLUSH_SIZE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.INT
  }

  @Test
  def checkRotateIntervalMS(): Unit = {
    val definition = hdfsSink.settingDefinitions().get(ROTATE_INTERVAL_MS_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultLong() shouldBe ROTATE_INTERVAL_MS_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.LONG
  }

  @Test
  def checkFileNeedHeader(): Unit = {
    val definition = hdfsSink.settingDefinitions().get(FILE_NEED_HEADER_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultBoolean() shouldBe FILE_NEED_HEADER_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.BOOLEAN
  }

  @Test
  def checkFileEncode(): Unit = {
    val definition = hdfsSink.settingDefinitions().get(FILE_ENCODE_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultString() shouldBe FILE_ENCODE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def testNormal(): Unit = {
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val topicKey     = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val response = result(
      connectorAdmin
        .connectorValidator()
        .numberOfTasks(1)
        .connectorKey(connectorKey)
        .topicKey(topicKey)
        .settings(
          Map(
            HDFS_URL_KEY           -> s"file://${testUtil.hdfs.tmpDirectory}",
            OUTPUT_FOLDER_KEY      -> s"file://${testUtil.hdfs.tmpDirectory}",
            FLUSH_SIZE_KEY         -> "10",
            ROTATE_INTERVAL_MS_KEY -> "4000"
          )
        )
        .connectorClass(classOf[HDFSSink])
        .run()
    )

    response.settings().size should not be 0
    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED
    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED
    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.OPTIONAL
    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.COLUMNS_DEFINITION.key())
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED
    response
      .settings()
      .asScala
      .filter(_.definition().key() == ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key())
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED
    response.errorCount() shouldBe 0
  }
}
