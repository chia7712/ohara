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

package oharastream.ohara.connector.jdbc.source

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.setting.SettingDef.{Necessary, Permission, Reference}
import oharastream.ohara.common.setting.{ConnectorKey, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestJDBCSourceConnectorDefinition extends WithBrokerWorker {
  private[this] val jdbcSource                 = new JDBCSourceConnector
  private[this] val connectorAdmin             = ConnectorAdmin(testUtil().workersConnProps())
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  @Test
  def checkDbURL(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_URL_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkDbUserName(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_USERNAME_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkDbPassword(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_PASSWORD_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.PASSWORD
  }

  @Test
  def checkFetchDataSize(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(FETCH_DATA_SIZE_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultInt shouldBe FETCH_DATA_SIZE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.INT
  }

  @Test
  def checkFlushDataSize(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(FLUSH_DATA_SIZE_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultInt shouldBe FLUSH_DATA_SIZE_DEFAULT
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.INT
  }

  @Test
  def checkTableName(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_TABLENAME_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.JDBC_TABLE
  }

  @Test
  def checkCatalogPattern(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_CATALOG_PATTERN_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkSchemaPattern(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(DB_SCHEMA_PATTERN_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkTimeStampColumnName(): Unit = {
    val definition = jdbcSource.settingDefinitions().get(TIMESTAMP_COLUMN_NAME_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def testSource(): Unit = {
    val url: String                 = "jdbc:postgresql://localhost:5432/postgres"
    val userName: String            = "user1"
    val password: String            = "123456"
    val tableName: String           = "table1"
    val timeStampColumnName: String = "COLUMN1"
    val topicKey                    = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val connectorKey                = ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    val response = result(
      connectorAdmin
        .connectorValidator()
        .connectorKey(connectorKey)
        .numberOfTasks(3)
        .topicKey(topicKey)
        .settings(
          Map(
            DB_URL_KEY                -> url,
            DB_USERNAME_KEY           -> userName,
            DB_PASSWORD_KEY           -> password,
            DB_TABLENAME_KEY          -> tableName,
            TIMESTAMP_COLUMN_NAME_KEY -> timeStampColumnName,
            FETCH_DATA_SIZE_KEY       -> "1000",
            FLUSH_DATA_SIZE_KEY       -> "1000"
          )
        )
        .connectorClass(classOf[JDBCSourceConnector])
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

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_URL_KEY)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_USERNAME_KEY)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_PASSWORD_KEY)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_TABLENAME_KEY)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == TIMESTAMP_COLUMN_NAME_KEY)
      .head
      .definition()
      .necessary() shouldBe Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_CATALOG_PATTERN_KEY)
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED

    response
      .settings()
      .asScala
      .filter(_.value().key() == DB_SCHEMA_PATTERN_KEY)
      .head
      .definition()
      .necessary() should not be Necessary.REQUIRED

    response.errorCount() shouldBe 0
  }
}
