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

import java.sql.{Statement, Timestamp}

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType, Row}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector.{RowSourceContext, TaskSetting}
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestTimestampQueryHandler extends OharaTest {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "TABLE1"
  private[this] val timestampColumnName = "COLUMN1"

  @BeforeEach
  def setup(): Unit = {
    client.connection.setAutoCommit(false)
    val column1 = RdbColumn("COLUMN1", "TIMESTAMP(6)", true)
    val column2 = RdbColumn("COLUMN2", "varchar(45)", false)
    val column3 = RdbColumn("COLUMN3", "VARCHAR(45)", false)
    val column4 = RdbColumn("COLUMN4", "integer", false)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()
    try {
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:01', 'a21', 'a22', 2)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:02', 'a31', 'a32', 3)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:03.12', 'a41', 'a42', 4)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:04.123456', 'a51', 'a52', 5)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(COLUMN1,COLUMN2,COLUMN3,COLUMN4) VALUES(NOW() + INTERVAL 3 DAY, 'a41', 'a42', 4)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName(column1,column2,column3,column4) VALUES(NOW() + INTERVAL 1 DAY, 'a51', 'a52', 5)"
      )
    } finally Releasable.close(statement)
  }

  @Test
  def testRowTimestamp(): Unit = {
    val schema: Seq[Column]                    = Seq(Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Timestamp]] = Seq(ColumnInfo("COLUMN1", "timestamp", new Timestamp(0)))
    val row0: Row =
      TimestampQueryHandler.builder.config(JDBCSourceConnectorConfig(taskSetting())).build().row(schema, columnInfo)
    row0.cell("COLUMN1").value.toString shouldBe "1970-01-01 08:00:00.0"
  }

  @Test
  def testRowInt(): Unit = {
    val schema: Seq[Column]              = Seq(Column.builder().name("COLUMN1").dataType(DataType.INT).order(0).build())
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", Integer.valueOf(100)))
    val row0: Row =
      TimestampQueryHandler.builder.config(JDBCSourceConnectorConfig(taskSetting())).build().row(schema, columnInfo)
    row0.cell("COLUMN1").value shouldBe 100
  }

  @Test
  def testCellOrder(): Unit = {
    val schema: Seq[Column] = Seq(
      Column.builder().name("c1").dataType(DataType.INT).order(1).build(),
      Column.builder().name("c0").dataType(DataType.INT).order(0).build()
    )
    val columnInfo: Seq[ColumnInfo[Int]] =
      Seq(ColumnInfo("c1", "int", Integer.valueOf(100)), ColumnInfo("c0", "int", Integer.valueOf(50)))
    val cells = TimestampQueryHandler.builder
      .config(JDBCSourceConnectorConfig(taskSetting()))
      .build()
      .row(schema, columnInfo)
      .cells()
      .asScala
    cells.head.name shouldBe "c0"
    cells.head.value shouldBe 50
    cells(1).name shouldBe "c1"
    cells(1).value shouldBe 100
  }

  @Test
  def testRowNewName(): Unit = {
    val schema: Seq[Column] = Seq(
      Column.builder().name("COLUMN1").newName("COLUMN100").dataType(DataType.INT).order(0).build()
    )
    val columnInfo: Seq[ColumnInfo[Int]] = Seq(ColumnInfo("COLUMN1", "int", Integer.valueOf(100)))
    val row0: Row =
      TimestampQueryHandler.builder.config(JDBCSourceConnectorConfig(taskSetting())).build().row(schema, columnInfo)
    row0.cell("COLUMN100").value shouldBe 100
  }

  @Test
  def testIsCompletedFalse(): Unit = {
    val key                       = s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0"
    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    (0 to 4).foreach { i =>
      val queryHandler = mockQueryHandler(key, i)
      queryHandler.completed(key, startTimestamp, stopTimestamp) shouldBe false
    }
  }

  @Test
  def testIsCompletedTrue(): Unit = {
    val key                       = s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0"
    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    mockQueryHandler(key, 5).completed(key, startTimestamp, stopTimestamp) shouldBe true
  }

  @Test
  def testIsCompletedException(): Unit = {
    val key                       = s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0"
    val startTimestamp: Timestamp = Timestamp.valueOf("2018-09-01 00:00:00")
    val stopTimestamp: Timestamp  = Timestamp.valueOf("2018-09-02 00:00:00")
    an[IllegalArgumentException] should be thrownBy
      mockQueryHandler(key, 6).completed(key, startTimestamp, stopTimestamp)
  }

  @AfterEach
  def afterTest(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
      Releasable.close(statement)
    }
    Releasable.close(client)
    Releasable.close(db)
  }

  @Test
  def testConvertToValue(): Unit = {
    val queryHandler = TimestampQueryHandler.builder.config(JDBCSourceConnectorConfig(taskSetting())).build()
    queryHandler
      .convertToValue(Column.builder.name("COLUMN1").dataType(DataType.BOOLEAN).build(), true)
      .isInstanceOf[java.lang.Boolean] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN2").dataType(DataType.SHORT).build(), 1.toShort)
      .isInstanceOf[java.lang.Short] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN3").dataType(DataType.INT).build(), 2.toInt)
      .isInstanceOf[java.lang.Integer] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN4").dataType(DataType.LONG).build(), 3.toLong)
      .isInstanceOf[java.lang.Long] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN5").dataType(DataType.FLOAT).build(), 4.0.toFloat)
      .isInstanceOf[java.lang.Float] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN6").dataType(DataType.DOUBLE).build(), 5.0.toDouble)
      .isInstanceOf[java.lang.Double] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN7").dataType(DataType.BYTE).build(), 100.toByte)
      .isInstanceOf[java.lang.Byte] shouldBe true

    queryHandler
      .convertToValue(
        Column.builder.name("COLUMN8").dataType(DataType.BYTES).build(),
        "test".getBytes.map(x => java.lang.Byte.valueOf(x))
      )
      .isInstanceOf[Array[java.lang.Byte]] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN9").dataType(DataType.STRING).build(), "test")
      .isInstanceOf[String] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN10").dataType(DataType.OBJECT).build(), 123)
      .isInstanceOf[Object] shouldBe true

    queryHandler
      .convertToValue(Column.builder.name("COLUMN10").dataType(DataType.OBJECT).build(), "test")
      .isInstanceOf[Object] shouldBe true
  }

  private[this] def mockQueryHandler(key: String, value: Int): TimestampQueryHandler = {
    val rowSourceContext          = Mockito.mock(classOf[RowSourceContext])
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> value.toString)
    when(
      rowSourceContext.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    val queryHandler = TimestampQueryHandler.builder
      .config(JDBCSourceConnectorConfig(taskSetting()))
      .rowSourceContext(rowSourceContext)
      .schema(Seq.empty)
      .topics(Seq.empty)
      .build()
    queryHandler.offsetCache.loadIfNeed(rowSourceContext, key)
    queryHandler
  }

  private[this] def taskSetting(): TaskSetting = {
    val taskSetting: TaskSetting = Mockito.mock(classOf[TaskSetting])
    when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
    when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
    when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
    when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
    when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
    when(taskSetting.stringOption(INCREMENT_COLUMN_NAME_KEY)).thenReturn(java.util.Optional.empty[String]())
    when(taskSetting.intOption(FETCH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY)).thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
    when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
    when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))
    when(taskSetting.columns).thenReturn(
      Seq(
        Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build(),
        Column.builder().name("COLUMN2").dataType(DataType.STRING).order(1).build(),
        Column.builder().name("COLUMN4").dataType(DataType.INT).order(3).build()
      ).asJava
    )
    when(taskSetting.topicKeys()).thenReturn(Set(TopicKey.of("g", "topic1")).asJava)
    taskSetting
  }
}
