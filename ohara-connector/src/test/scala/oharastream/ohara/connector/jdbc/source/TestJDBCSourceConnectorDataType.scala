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

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.{Column, DataType, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class TestJDBCSourceConnectorDataType extends With3Brokers3Workers {
  private[this] val db                         = Database.local()
  private[this] val client                     = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName                  = "table1"
  private[this] val timestampColumnName        = "column1"
  private[this] val connectorAdmin             = ConnectorAdmin(testUtil.workersConnProps)
  private[this] var connectorKey: ConnectorKey = _
  @BeforeEach
  def setup(): Unit = {
    val connection = client.connection
    val statement  = connection.createStatement()
    connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    statement.executeUpdate(
      s"create table $tableName($timestampColumnName timestamp(6)," +
        "column2 longblob," +
        "column3 bit," +
        "column4 tinyint," +
        "column5 boolean," +
        "column6 BIGINT," +
        "column7 float," +
        "column8 double," +
        "column9 decimal," +
        "column10 date," +
        "column11 time," +
        "column12 ENUM('A','B')," +
        "column13 LONGTEXT)"
    )

    val sql  = s"INSERT INTO table1 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val stmt = connection.prepareStatement(sql)
    try {
      val binaryData = "some binary data ...".getBytes()
      stmt.setString(1, "2018-10-01 00:00:00")
      stmt.setBytes(2, binaryData)
      stmt.setByte(3, 1.toByte)
      stmt.setInt(4, 100)
      stmt.setBoolean(5, false)
      stmt.setLong(6, 1000)
      stmt.setFloat(7, 200)
      stmt.setDouble(8, 2000)
      stmt.setBigDecimal(9, java.math.BigDecimal.valueOf(10000))
      stmt.setDate(10, java.sql.Date.valueOf("2018-10-01"))
      stmt.setTime(11, java.sql.Time.valueOf("11:00:00"))
      stmt.setString(12, "B")
      stmt.setString(13, "aaaaaaaaaa")
      stmt.executeUpdate()
    } finally Releasable.close(stmt)
  }

  @Test
  def testSettingColumns(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    val columns = Seq(
      Column.builder.name("column1").newName("c1").dataType(DataType.OBJECT).build(),
      Column.builder.name("column2").newName("c2").dataType(DataType.BYTES).build(),
      Column.builder.name("column3").newName("c3").dataType(DataType.BOOLEAN).build(),
      Column.builder.name("column4").newName("c4").dataType(DataType.INT).build(),
      Column.builder.name("column5").newName("c5").dataType(DataType.BOOLEAN).build(),
      Column.builder.name("column6").newName("c6").dataType(DataType.LONG).build(),
      Column.builder.name("column7").newName("c7").dataType(DataType.FLOAT).build(),
      Column.builder.name("column8").newName("c8").dataType(DataType.DOUBLE).build(),
      Column.builder.name("column9").newName("c9").dataType(DataType.OBJECT).build(),
      Column.builder.name("column10").newName("c10").dataType(DataType.OBJECT).build(),
      Column.builder.name("column11").newName("c11").dataType(DataType.OBJECT).build(),
      Column.builder.name("column12").newName("c12").dataType(DataType.STRING).build(),
      Column.builder.name("column13").newName("c13").dataType(DataType.STRING).build()
    )
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(3)
        .settings(props.toMap)
        .columns(columns)
        .create()
    )
    val record = pollData(topicKey, Duration(30, TimeUnit.SECONDS), 1)
    record.size shouldBe 1
    val row0 = record.head.key.get
    row0.cell("c1").value.isInstanceOf[java.sql.Timestamp] shouldBe true
    row0.cell("c1").value.toString shouldBe "2018-10-01 00:00:00.0"

    row0.cell("c2").value.isInstanceOf[Array[java.lang.Byte]] shouldBe true
    row0.cell("c2").value shouldBe "some binary data ...".getBytes()

    row0.cell("c3").value.isInstanceOf[java.lang.Boolean] shouldBe true
    row0.cell("c3").value shouldBe true

    row0.cell("c4").value.isInstanceOf[java.lang.Integer] shouldBe true
    row0.cell("c4").value shouldBe 100

    row0.cell("c5").value.isInstanceOf[java.lang.Boolean] shouldBe true
    row0.cell("c5").value shouldBe false

    row0.cell("c6").value.isInstanceOf[java.lang.Long] shouldBe true
    row0.cell("c6").value shouldBe 1000

    row0.cell("c7").value.isInstanceOf[java.lang.Float] shouldBe true
    row0.cell("c7").value shouldBe 200

    row0.cell("c8").value.isInstanceOf[java.lang.Double] shouldBe true
    row0.cell("c8").value shouldBe 2000

    row0.cell("c9").value.isInstanceOf[java.math.BigDecimal] shouldBe true
    row0.cell("c9").value.asInstanceOf[java.math.BigDecimal].intValue() shouldBe 10000

    row0.cell("c10").value.isInstanceOf[java.sql.Date] shouldBe true
    row0.cell("c10").value shouldBe java.sql.Date.valueOf("2018-10-01")

    row0.cell("c11").value.isInstanceOf[java.sql.Time] shouldBe true
    row0.cell("c11").value shouldBe java.sql.Time.valueOf("11:00:00")

    row0.cell("c12").value.isInstanceOf[java.lang.String] shouldBe true
    row0.cell("c12").value shouldBe "B"

    row0.cell("c13").value.isInstanceOf[java.lang.String] shouldBe true
    row0.cell("c13").value shouldBe "aaaaaaaaaa"
  }

  @Test
  def testDefaultColumns(): Unit = {
    val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(3)
        .settings(props.toMap)
        .create()
    )

    val record = pollData(topicKey, Duration(30, TimeUnit.SECONDS), 1)
    val row0   = record.head.key.get

    record.size shouldBe 1

    // Test timestamp type
    row0.cell(0).value.isInstanceOf[java.sql.Timestamp] shouldBe true
    row0.cell(0).value.toString shouldBe "2018-10-01 00:00:00.0"

    // Test byte array type
    row0.cell(1).value.isInstanceOf[Array[java.lang.Byte]] shouldBe true
    new String(row0.cell(1).value.asInstanceOf[Array[java.lang.Byte]].map(x => Byte.unbox(x))) shouldBe "some binary data ..."

    // Test bit type
    row0.cell(2).value.isInstanceOf[java.lang.Boolean] shouldBe true
    row0.cell(2).value shouldBe true

    // Test tinyint type
    row0.cell(3).value.isInstanceOf[java.lang.Integer] shouldBe true
    row0.cell(3).value shouldBe 100

    // Test boolean type
    row0.cell(4).value.isInstanceOf[java.lang.Boolean] shouldBe true
    row0.cell(4).value shouldBe false

    // Test long type
    row0.cell(5).value.isInstanceOf[java.lang.Long] shouldBe true
    row0.cell(5).value shouldBe 1000

    // Test float type
    row0.cell(6).value.isInstanceOf[java.lang.Float] shouldBe true
    row0.cell(6).value shouldBe 200.0

    // Test double type
    row0.cell(7).value.isInstanceOf[java.lang.Double] shouldBe true
    row0.cell(7).value shouldBe 2000.0

    // Test big decimal type
    row0.cell(8).value.isInstanceOf[java.math.BigDecimal] shouldBe true
    row0.cell(8).value shouldBe java.math.BigDecimal.valueOf(10000)

    // Test date type
    row0.cell(9).value.isInstanceOf[java.sql.Date] shouldBe true
    row0.cell(9).value shouldBe java.sql.Date.valueOf("2018-10-01")

    // Test time type
    row0.cell(10).value.isInstanceOf[java.sql.Time] shouldBe true
    row0.cell(10).value shouldBe java.sql.Time.valueOf("11:00:00")

    // Test enum type
    row0.cell(11).value.isInstanceOf[java.lang.String] shouldBe true
    row0.cell(11).value.toString shouldBe "B"

    // Test longtext type
    row0.cell(12).value.isInstanceOf[java.lang.String] shouldBe true
    row0.cell(12).value.toString shouldBe "aaaaaaaaaa"
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

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

  @AfterEach
  def tearDown(): Unit = {
    result(connectorAdmin.delete(connectorKey))
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
