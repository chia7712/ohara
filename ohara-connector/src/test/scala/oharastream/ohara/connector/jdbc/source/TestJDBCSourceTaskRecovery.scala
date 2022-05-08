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
import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.connector.{RowSourceRecord, TaskSetting}
import oharastream.ohara.testing.service.Database
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestJDBCSourceTaskRecovery extends OharaTest {
  private[this] val db                                       = Database.local()
  private[this] val client                                   = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName                                = "TABLE1"
  private[this] val timestampColumnName                      = "COLUMN1"
  private[this] val jdbcSourceTask: JDBCSourceTask           = new JDBCSourceTask()
  private[this] val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
  private[this] val taskSetting: TaskSetting                 = Mockito.mock(classOf[TaskSetting])
  private[this] val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])

  @BeforeEach
  def setup(): Unit = {
    val column1 = RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
    val column2 = RdbColumn("COLUMN2", "varchar(45)", false)
    val column3 = RdbColumn("COLUMN3", "VARCHAR(45)", false)
    val column4 = RdbColumn("COLUMN4", "integer", true)

    client.createTable(tableName, Seq(column1, column2, column3, column4))
    val statement: Statement = db.connection.createStatement()
    try {
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a11', 'a12', 1)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a21', 'a22', 2)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a31', 'a32', 3)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:00', 'a41', 'a42', 4)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:02', 'a51', 'a52', 5)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-01 00:00:03.12', 'a61', 'a62', 6)"
      )
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-02 00:00:04', 'a71', 'a72', 7)"
      )

      // Mock JDBC Source Task
      when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader)
      jdbcSourceTask.initialize(taskContext.asInstanceOf[SourceTaskContext])

      when(taskSetting.stringValue(DB_URL_KEY)).thenReturn(db.url)
      when(taskSetting.stringValue(DB_USERNAME_KEY)).thenReturn(db.user)
      when(taskSetting.stringValue(DB_PASSWORD_KEY)).thenReturn(db.password)
      when(taskSetting.stringValue(DB_TABLENAME_KEY)).thenReturn(tableName)
      when(taskSetting.stringOption(DB_SCHEMA_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
      when(taskSetting.stringOption(DB_CATALOG_PATTERN_KEY)).thenReturn(java.util.Optional.empty[String]())
      when(taskSetting.stringValue(TIMESTAMP_COLUMN_NAME_KEY)).thenReturn(timestampColumnName)
      when(taskSetting.stringOption(INCREMENT_COLUMN_NAME_KEY)).thenReturn(java.util.Optional.empty[String]())
      when(taskSetting.intOption(FETCH_DATA_SIZE_KEY))
        .thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
      when(taskSetting.intOption(FLUSH_DATA_SIZE_KEY))
        .thenReturn(java.util.Optional.of(java.lang.Integer.valueOf(2000)))
      when(taskSetting.intOption(TASK_HASH_KEY)).thenReturn(java.util.Optional.of(0))
      when(taskSetting.intOption(TASK_TOTAL_KEY)).thenReturn(java.util.Optional.of(1))

      val columns: Seq[Column] = Seq(
        Column.builder().name("COLUMN1").dataType(DataType.OBJECT).order(0).build(),
        Column.builder().name("COLUMN2").dataType(DataType.STRING).order(1).build(),
        Column.builder().name("COLUMN4").dataType(DataType.INT).order(3).build()
      )

      when(taskSetting.columns).thenReturn(columns.asJava)
      when(taskSetting.topicKeys()).thenReturn(Set(TopicKey.of("g", "topic1")).asJava)
    } finally Releasable.close(statement)
  }

  @Test
  def testNormal(): Unit = {
    jdbcSourceTask.run(taskSetting)
    val partition1Rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq

    partition1Rows.head.row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    partition1Rows.head.row.cell(1).value shouldBe "a11"
    partition1Rows.head.row.cell(2).value shouldBe 1

    partition1Rows(1).row.cell(0).value.toString shouldBe "2018-09-01 00:00:00.0"
    partition1Rows(1).row.cell(1).value shouldBe "a21"
    partition1Rows(1).row.cell(2).value shouldBe 2

    val partition2Rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    partition2Rows.last.row.cell(0).value.toString shouldBe "2018-09-02 00:00:04.0"
    partition2Rows.last.row.cell(1).value shouldBe "a71"
    partition2Rows.last.row.cell(2).value shouldBe 7

    //Test offset value for JDBC Source Connector
    partition1Rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe s"1"
    })

    partition1Rows(1).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe s"2"
    })

    partition1Rows(2).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe s"3"
    })

    partition1Rows(3).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe s"4"
    })

    partition1Rows(4).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "5"
    })

    partition1Rows(5).sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "6"
    })

    partition2Rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "1"
    })
  }

  @Test
  def testRestartJDBCSourceConnector_1(): Unit = {
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "0")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    jdbcSourceTask.run(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows.size shouldBe 6
  }

  @Test
  def testRestartJDBCSourceConnector_2(): Unit = {
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "1")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    jdbcSourceTask.run(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows.size shouldBe 5
  }

  @Test
  def testRestartJDBCSourceConnector_3(): Unit = {
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "3")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    jdbcSourceTask.run(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows.size shouldBe 3
  }

  @Test
  def testRestartJDBCSourceConnector_4(): Unit = {
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "5")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    jdbcSourceTask.run(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows.size shouldBe 1
    rows.head.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "6"
    })
  }

  @Test
  def testInsertData(): Unit = {
    val statement: Statement = db.connection.createStatement()
    statement.executeUpdate(
      s"INSERT INTO $tableName($timestampColumnName,COLUMN2,COLUMN3,COLUMN4) VALUES('2018-09-03 00:00:04', 'a81', 'a82', 8)"
    )

    val partition1: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "5")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(partition1.asJava)

    jdbcSourceTask.run(taskSetting)
    val partition1Rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq

    partition1Rows.size shouldBe 1
    partition1Rows.last.sourceOffset.asScala.foreach(x => {
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "6"
    })

    val partition2: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "0")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-02 00:00:00.0~2018-09-03 00:00:00.0").asJava
      )
    ).thenReturn(partition2.asJava)
    val partition2Rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq

    partition2Rows.head.row.cell(0).value.toString shouldBe "2018-09-02 00:00:04.0"
    partition2Rows.head.row.cell(2).name shouldBe "COLUMN4"
    partition2Rows.head.row.cell(2).value shouldBe 7

    val partition3: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "0")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-03 00:00:00.0~2018-09-04 00:00:00.0").asJava
      )
    ).thenReturn(partition3.asJava)
    val partition3Rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    partition3Rows.last.row.cell(1).name shouldBe "COLUMN2"
    partition3Rows.last.row.cell(1).value shouldBe "a81"
  }

  @AfterEach
  def tearDown(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
      Releasable.close(statement)
    }
    Releasable.close(client)
    Releasable.close(db)
  }
}
