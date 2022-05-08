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

class TestJDBCSourceTaskOffset extends OharaTest {
  private[this] val db                  = Database.local()
  private[this] val client              = DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val tableName           = "TABLE1"
  private[this] val timestampColumnName = "COLUMN1"

  private[this] val jdbcSourceTask: JDBCSourceTask           = new JDBCSourceTask()
  private[this] val taskContext: SourceTaskContext           = Mockito.mock(classOf[SourceTaskContext])
  private[this] val taskSetting: TaskSetting                 = Mockito.mock(classOf[TaskSetting])
  private[this] val offsetStorageReader: OffsetStorageReader = Mockito.mock(classOf[OffsetStorageReader])

  @BeforeEach
  def setup(): Unit = {
    val column2 = "COLUMN2"
    val dbColumns = Seq(
      RdbColumn(timestampColumnName, "TIMESTAMP(6)", false),
      RdbColumn(column2, "varchar(45)", true)
    )
    client.createTable(tableName, dbColumns)
    val statement: Statement = db.connection.createStatement()
    try {
      statement.executeUpdate(
        s"INSERT INTO $tableName($timestampColumnName, column2) VALUES('2018-09-01 00:00:00', '1')"
      )
      (2 to 8).foreach { i =>
        val sql = s"INSERT INTO $tableName($timestampColumnName, column2) VALUES('2018-09-01 00:00:01', '$i')"
        statement.executeUpdate(sql)
      }

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
        Column.builder().name(timestampColumnName).dataType(DataType.OBJECT).order(0).build(),
        Column.builder().name(column2).dataType(DataType.STRING).order(1).build()
      )

      when(taskSetting.columns).thenReturn(columns.asJava)
      when(taskSetting.topicKeys()).thenReturn(Set(TopicKey.of("g", "topic1")).asJava)
    } finally Releasable.close(statement)
  }

  @Test
  def testOffset(): Unit = {
    val maps: Map[String, Object] = Map(JDBCOffsetCache.TABLE_OFFSET_KEY -> "4")
    when(
      offsetStorageReader.offset(
        Map(JDBCOffsetCache.TABLE_PARTITION_KEY -> s"$tableName:2018-09-01 00:00:00.0~2018-09-02 00:00:00.0").asJava
      )
    ).thenReturn(maps.asJava)

    jdbcSourceTask.run(taskSetting)
    val rows: Seq[RowSourceRecord] = jdbcSourceTask.pollRecords().asScala.toSeq
    rows.size shouldBe 4
    rows(0).sourceOffset.asScala.foreach { x =>
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "5"
    }

    rows(1).sourceOffset.asScala.foreach { x =>
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "6"
    }

    rows(2).sourceOffset.asScala.foreach { x =>
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "7"
    }

    rows(3).sourceOffset.asScala.foreach { x =>
      x._1 shouldBe JDBCOffsetCache.TABLE_OFFSET_KEY
      x._2 shouldBe "8"
    }
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
}
