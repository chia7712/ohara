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
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.kafka.connector.TaskSetting
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
class TestJDBCSourceConnectorConfig extends OharaTest {
  private[this] def jdbcConfig(settings: Map[String, String]): JDBCSourceConnectorConfig =
    JDBCSourceConnectorConfig(TaskSetting.of(settings.asJava))

  @Test
  def testSettingProperty(): Unit = {
    val map1: Map[String, String] =
      Map(
        DB_URL_KEY                -> "jdbc:mysql://localhost/test",
        DB_USERNAME_KEY           -> "root",
        DB_PASSWORD_KEY           -> "123456",
        DB_TABLENAME_KEY          -> "TABLE1",
        DB_SCHEMA_PATTERN_KEY     -> "schema1",
        TIMESTAMP_COLUMN_NAME_KEY -> "CDC_TIMESTAMP"
      )

    val jdbcSourceConnectorConfig = jdbcConfig(map1)
    jdbcSourceConnectorConfig.dbURL shouldBe "jdbc:mysql://localhost/test"
    jdbcSourceConnectorConfig.dbUserName shouldBe "root"
    jdbcSourceConnectorConfig.dbPassword shouldBe "123456"
    jdbcSourceConnectorConfig.dbTableName shouldBe "TABLE1"
    jdbcSourceConnectorConfig.dbSchemaPattern.get shouldBe "schema1"
    jdbcSourceConnectorConfig.timestampColumnName shouldBe "CDC_TIMESTAMP"
    jdbcSourceConnectorConfig.fetchDataSize shouldBe FETCH_DATA_SIZE_DEFAULT
  }

  @Test
  def testFetchDataSize(): Unit = {
    val map1: Map[String, String] =
      Map(
        DB_URL_KEY                -> "jdbc:mysql://localhost/test",
        DB_USERNAME_KEY           -> "root",
        DB_PASSWORD_KEY           -> "123456",
        DB_TABLENAME_KEY          -> "TABLE1",
        DB_SCHEMA_PATTERN_KEY     -> "schema1",
        FETCH_DATA_SIZE_KEY       -> "500",
        TIMESTAMP_COLUMN_NAME_KEY -> "CDC_TIMESTAMP"
      )

    val jdbcSourceConnectorConfig = jdbcConfig(map1)
    jdbcSourceConnectorConfig.fetchDataSize shouldBe 500
  }
  @Test
  def testException(): Unit = {
    intercept[NoSuchElementException] {
      jdbcConfig(Map())
    }.getMessage shouldBe s"$DB_URL_KEY doesn't exist"

    intercept[NoSuchElementException] {
      jdbcConfig(Map(DB_URL_KEY -> "jdbc:mysql://localhost:3306"))
    }.getMessage shouldBe s"$DB_USERNAME_KEY doesn't exist"

    intercept[NoSuchElementException] {
      jdbcConfig(Map(DB_URL_KEY -> "jdbc:mysql://localhost/test", DB_USERNAME_KEY -> "root"))
    }.getMessage shouldBe s"$DB_PASSWORD_KEY doesn't exist"
  }

  @Test
  def testCatalogAndSchema(): Unit = {
    val config = JDBCSourceConnectorConfig(
      dbURL = "123",
      dbUserName = "123",
      dbPassword = "123",
      dbTableName = "123",
      dbCatalogPattern = None,
      dbSchemaPattern = None,
      fetchDataSize = 1000,
      flushDataSize = 1000,
      timestampColumnName = "123",
      incrementColumnName = None,
      taskTotal = 0,
      taskHash = 0
    )

    config.toMap.contains(DB_CATALOG_PATTERN_KEY) shouldBe false
    config.toMap.contains(DB_SCHEMA_PATTERN_KEY) shouldBe false

    val configMap = Map[String, String](
      DB_URL_KEY                -> "aa",
      DB_USERNAME_KEY           -> "aa",
      DB_PASSWORD_KEY           -> "aa",
      DB_TABLENAME_KEY          -> "aa",
      DB_CATALOG_PATTERN_KEY    -> "aa",
      DB_SCHEMA_PATTERN_KEY     -> "aa",
      TIMESTAMP_COLUMN_NAME_KEY -> "aa",
      INCREMENT_COLUMN_NAME_KEY -> "aa"
    )

    jdbcConfig(configMap).dbSchemaPattern.isEmpty shouldBe false
    jdbcConfig(configMap).dbCatalogPattern.isEmpty shouldBe false
    jdbcConfig(configMap).incrementColumnName.isEmpty shouldBe false

    val configMap2 = Map[String, String](
      DB_URL_KEY                -> "aa",
      DB_USERNAME_KEY           -> "aa",
      DB_PASSWORD_KEY           -> "aa",
      DB_TABLENAME_KEY          -> "aa",
      TIMESTAMP_COLUMN_NAME_KEY -> "aa"
    )

    jdbcConfig(configMap2).dbSchemaPattern.isEmpty shouldBe true
    jdbcConfig(configMap2).dbCatalogPattern.isEmpty shouldBe true
    jdbcConfig(configMap2).incrementColumnName.isEmpty shouldBe true

    jdbcConfig(configMap2) shouldBe jdbcConfig(configMap2)
    jdbcConfig(configMap2) shouldBe jdbcConfig(jdbcConfig(configMap2).toMap)
  }
}
