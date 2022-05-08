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
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.client.kafka.WorkerJson.ConnectorCreationResponse
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.Consumer
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.testing.With3Brokers3Workers
import oharastream.ohara.testing.service.Database
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class TestJDBCSourceConnectorTimeRange extends With3Brokers3Workers {
  protected[this] val db: Database        = Database.local()
  protected[this] val tableName           = "table1"
  protected[this] val timestampColumnName = "c0"

  private[this] val client: DatabaseClient =
    DatabaseClient.builder.url(db.url()).user(db.user()).password(db.password()).build
  private[this] val beginIndex      = 1
  private[this] val totalColumnSize = 3
  private[this] val columns = Seq(
    RdbColumn(timestampColumnName, "TIMESTAMP(6)", false)
  ) ++
    (beginIndex to totalColumnSize).map { index =>
      if (index == 1) RdbColumn(s"c$index", "VARCHAR(45)", true)
      else RdbColumn(s"c$index", "VARCHAR(45)", false)
    }
  private[this] val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @CsvSource(
    value = Array(
      "1598506853739, 1598852453739,  3600000, 1", // 5 days ~ 1 days, split by 1 hour, 1 task
      "1598506853739, 1598852453739,  3600000, 3", // 5 days ~ 1 days, split by 1 hour, 3 task
      "1598506853739, 9998852453739,  3600000, 3"  // 5 days ~ future, split by 1 hour, 3 task
    )
  )
  def testConnector(startTimestamp: Long, stopTimestamp: Long, incrementTimestamp: Long, taskNumber: Int): Unit = {
    client.createTable(tableName, columns)
    insertData(startTimestamp.to(Math.min(stopTimestamp, CommonUtils.current())).by(incrementTimestamp).map { value =>
      new Timestamp(value)
    })
    val connectorKey = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
    try {
      val topicKey = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
      result(createConnector(connectorAdmin, connectorKey, topicKey, taskNumber))

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
        val records1 = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimeResultCount()).asScala
        records1.size shouldBe tableCurrentTimeResultCount()

        TimeUnit.SECONDS.sleep(10)

        insertData((1 to 100).map { _ =>
          new Timestamp(CommonUtils.current())
        })

        consumer.seekToBeginning()
        val records2 = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimeResultCount()).asScala
        records2.size shouldBe tableCurrentTimeResultCount()

        TimeUnit.SECONDS.sleep(10)
        insertData(Seq(new Timestamp(CommonUtils.current())))
        consumer.seekToBeginning()
        val records3 = consumer.poll(java.time.Duration.ofSeconds(60), tableCurrentTimeResultCount()).asScala

        tableData(
          records3
            .map { record =>
              record.key().get().cell(timestampColumnName).value().toString
            }
            .sorted[String]
            .toSeq
        )
      } finally Releasable.close(consumer)
    } finally result(connectorAdmin.delete(connectorKey))
  }

  private[this] def tableCurrentTimeResultCount(): Int = {
    val preparedStatement =
      client.connection.prepareStatement(s"SELECT count(*) FROM $tableName WHERE $timestampColumnName <= ?")
    preparedStatement.setTimestamp(1, new Timestamp(CommonUtils.current()))

    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getInt(1)
        else 0
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  private[this] def tableData(topicRecords: Seq[String]): Unit = {
    val preparedStatement = client.connection.prepareStatement(
      s"SELECT * FROM $tableName WHERE $timestampColumnName <= ? ORDER BY $timestampColumnName"
    )
    preparedStatement.setTimestamp(1, new Timestamp(CommonUtils.current()))

    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        Iterator
          .continually(resultSet)
          .takeWhile(_.next())
          .zipWithIndex
          .foreach {
            case (result, index) =>
              result.getTimestamp(timestampColumnName).toString shouldBe topicRecords(index)
          }
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  private[this] def insertData(timestamps: Seq[Timestamp]): Unit = {
    val sql               = s"INSERT INTO $tableName($timestampColumnName, c1, c2, c3) VALUES (?, ?, ?, ?)"
    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      timestamps.foreach { timestamp =>
        preparedStatement.setTimestamp(1, timestamp)
        rowData().asScala.zipWithIndex.foreach {
          case (result, index) => preparedStatement.setString(index + 2, result.value().toString)
        }
        preparedStatement.execute()
      }
    } finally Releasable.close(preparedStatement)
  }

  @AfterEach
  def after(): Unit = {
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
      Releasable.close(statement)
    }
    Releasable.close(client)
    Releasable.close(db)
  }

  private[this] def rowData(): Row = {
    Row.of(
      (beginIndex to totalColumnSize).map(index => {
        Cell.of(s"c$index", CommonUtils.randomString())
      }): _*
    )
  }

  private[this] def result[T](future: Future[T]): T = Await.result(future, Duration(20, TimeUnit.SECONDS))

  private[this] def createConnector(
    connectorAdmin: ConnectorAdmin,
    connectorKey: ConnectorKey,
    topicKey: TopicKey,
    taskNumber: Int
  ): Future[ConnectorCreationResponse] = {
    connectorAdmin
      .connectorCreator()
      .connectorKey(connectorKey)
      .connectorClass(classOf[JDBCSourceConnector])
      .topicKey(topicKey)
      .numberOfTasks(taskNumber)
      .settings(jdbcSourceConnectorProps.toMap)
      .create()
  }

  protected[this] def jdbcSourceConnectorProps: JDBCSourceConnectorConfig = {
    JDBCSourceConnectorConfig(
      TaskSetting.of(
        Map(
          DB_URL_KEY                -> db.url,
          DB_USERNAME_KEY           -> db.user,
          DB_PASSWORD_KEY           -> db.password,
          DB_TABLENAME_KEY          -> tableName,
          TIMESTAMP_COLUMN_NAME_KEY -> timestampColumnName,
          TASK_TOTAL_KEY            -> "0",
          TASK_HASH_KEY             -> "0"
        ).asJava
      )
    )
  }
}
