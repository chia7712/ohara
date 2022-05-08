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

import java.sql.Statement
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class TestMultipleJDBCSourceConnector extends With3Brokers3Workers {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "table1"
  private[this] val timestampColumnName = "column1"
  private[this] val connectorAdmin      = ConnectorAdmin(testUtil.workersConnProps)
  private[this] val connectorKey1       = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
  private[this] val connectorKey2       = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")

  @BeforeEach
  def setup(): Unit = {
    val column1 = RdbColumn("column1", "TIMESTAMP(6)", true)
    val column2 = RdbColumn("column2", "varchar(45)", false)
    val column3 = RdbColumn("column3", "VARCHAR(45)", false)
    val column4 = RdbColumn("column4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()
    try {
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:03.123456', 'a61', 'a62', 6)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES('2018-09-01 00:00:04.123', 'a71', 'a72', 7)"
      )
      statement.executeUpdate(s"INSERT INTO $tableName(column1) VALUES('2018-09-01 00:00:05')")

      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 3 MINUTE, 'a41', 'a42', 4)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)"
      )
    } finally Releasable.close(statement)
  }

  @Test
  def testRunningTwoConnector(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))

    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey1)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(3)
        .settings(props.toMap)
        .create()
    )

    val consumer =
      Consumer
        .builder()
        .topicKey(topicKey)
        .offsetFromBegin()
        .connectionProps(testUtil.brokersConnProps)
        .keySerializer(Serializer.ROW)
        .valueSerializer(Serializer.BYTES)
        .build()
    try {
      val record1 = consumer.poll(java.time.Duration.ofSeconds(30), 6).asScala
      record1.size shouldBe 6

      result(
        connectorAdmin
          .connectorCreator()
          .connectorKey(connectorKey2)
          .connectorClass(classOf[JDBCSourceConnector])
          .topicKey(topicKey)
          .numberOfTasks(3)
          .settings(props.toMap)
          .create()
      )

      consumer.seekToBeginning()
      val record2 = consumer.poll(java.time.Duration.ofSeconds(30), 12).asScala
      record2.size shouldBe 12

      val statement: Statement = db.connection.createStatement()
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1, column2, column3, column4) VALUES('2018-09-02 00:00:05', 'a81', 'a82', 8)"
      )

      consumer.seekToBeginning()
      val record3 = consumer.poll(java.time.Duration.ofSeconds(30), 14).asScala
      record3.size shouldBe 14

      val expectResult: Seq[String] = Seq(
        "2018-09-01 00:00:00.0,a11,a12,1",
        "2018-09-01 00:00:01.0,a21,a22,2",
        "2018-09-01 00:00:02.0,a31,a32,3",
        "2018-09-01 00:00:03.123456,a61,a62,6",
        "2018-09-01 00:00:04.123,a71,a72,7",
        "2018-09-01 00:00:05.0,null,null,0",
        "2018-09-01 00:00:00.0,a11,a12,1",
        "2018-09-01 00:00:01.0,a21,a22,2",
        "2018-09-01 00:00:02.0,a31,a32,3",
        "2018-09-01 00:00:03.123456,a61,a62,6",
        "2018-09-01 00:00:04.123,a71,a72,7",
        "2018-09-01 00:00:05.0,null,null,0",
        "2018-09-02 00:00:05.0,a81,a82,8",
        "2018-09-02 00:00:05.0,a81,a82,8"
      )
      val resultData: Seq[String] =
        record3.map(x => x.key.get).map(x => x.cells().asScala.map(_.value).mkString(",")).toSeq

      expectResult.indices.foreach(i => resultData(i) shouldBe expectResult(i))
    } finally consumer.close()
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

  @AfterEach
  def tearDown(): Unit = {
    result(connectorAdmin.delete(connectorKey2))
    result(connectorAdmin.delete(connectorKey1))
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
      Releasable.close(statement)
    }
    Releasable.close(client)
    Releasable.close(db)
  }

  private[this] val props = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        DB_URL_KEY                -> db.url,
        DB_USERNAME_KEY           -> db.user,
        DB_PASSWORD_KEY           -> db.password,
        DB_TABLENAME_KEY          -> tableName,
        TIMESTAMP_COLUMN_NAME_KEY -> timestampColumnName
      ).asJava
    )
  )
}
