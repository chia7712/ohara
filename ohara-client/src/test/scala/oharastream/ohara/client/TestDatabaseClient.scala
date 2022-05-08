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

package oharastream.ohara.client

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

class TestDatabaseClient extends OharaTest {
  private[this] val db = Database.local()

  private[this] val client = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build

  private[this] val increasedNumber = client.databaseType match {
    // postgresql generate one table called "xxx_pkey"
    case "postgresql" => 2
    case _            => 1
  }
  @Test
  def testList(): Unit = {
    val before    = client.tables().size
    val tableName = CommonUtils.randomString(10)
    val cf0       = RdbColumn("cf0", "INTEGER", true)
    val cf1       = RdbColumn("cf1", "INTEGER", false)
    val cf2       = RdbColumn("cf2", "INTEGER", false)
    client.createTable(tableName, Seq(cf2, cf0, cf1))
    try {
      val after = client.tables().size
      after - before shouldBe increasedNumber
    } finally client.dropTable(tableName)
  }

  @Test
  def testCreate(): Unit = {
    // postgresql use lower case...
    val tableName = CommonUtils.randomString(10)
    val cf0       = RdbColumn("cf0", "INTEGER", true)
    val cf1       = RdbColumn("cf1", "INTEGER", true)
    val cf2       = RdbColumn("cf2", "INTEGER", false)
    val before    = client.tables().size
    client.createTable(tableName, Seq(cf2, cf0, cf1))
    try {
      client.tables().size - before shouldBe increasedNumber
      val cfs = client.tableQuery.tableName(tableName).execute().head.columns
      cfs.size shouldBe 3
      cfs.filter(_.name == "cf0").head.pk shouldBe true
      cfs.filter(_.name == "cf1").head.pk shouldBe true
      cfs.filter(_.name == "cf2").head.pk shouldBe false
    } finally client.dropTable(tableName)
  }

  @Test
  def testDrop(): Unit = {
    val tableName = CommonUtils.randomString(10)
    val cf0       = RdbColumn("cf0", "INTEGER", true)
    val cf1       = RdbColumn("cf1", "INTEGER", false)
    client.createTable(tableName, Seq(cf0, cf1))
    val before = client.tables().size
    client.dropTable(tableName)
    before - client.tables().size shouldBe increasedNumber
  }

  @Test
  def nullUrl(): Unit = an[NullPointerException] should be thrownBy DatabaseClient.builder.url(null)

  @Test
  def emptyUrl(): Unit = an[IllegalArgumentException] should be thrownBy DatabaseClient.builder.url("")

  @Test
  def testUser(): Unit = {
    // USER is optional to jdbc so null and empty string are legal
    DatabaseClient.builder.user(null)
    DatabaseClient.builder.user("")
  }

  @Test
  def testPassword(): Unit = {
    // PASSWORD is optional to jdbc so null and empty string are legal
    DatabaseClient.builder.password(null)
    DatabaseClient.builder.password("")
  }

  @AfterEach
  def tearDown(): Unit = {
    Releasable.close(client)
    Releasable.close(db)
  }
}
