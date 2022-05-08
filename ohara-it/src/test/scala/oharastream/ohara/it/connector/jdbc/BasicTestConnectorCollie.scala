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

package oharastream.ohara.it.connector.jdbc

import java.io.File
import java.sql.{Statement, Timestamp}
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.Logger
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.source.{JDBCSourceConnector, JDBCSourceConnectorConfig}
import oharastream.ohara.it.ContainerPlatform.ResourceRef
import oharastream.ohara.it.{ContainerPlatform, IntegrationTest}
import oharastream.ohara.kafka.Consumer
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.{AfterEach, Tag}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

@Tag("integration-test-connector")
@EnabledIfEnvironmentVariable(named = "ohara.it.jar.folder", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.docker", matches = ".*")
private[jdbc] abstract class BasicTestConnectorCollie extends IntegrationTest {
  private[this] val log                    = Logger(classOf[BasicTestConnectorCollie])
  private[this] val inputDataTime          = 5000L
  private[this] val JAR_FOLDER_KEY: String = "ohara.it.jar.folder"
  private[this] val jarFolderPath          = sys.env(JAR_FOLDER_KEY)
  private[this] var client: DatabaseClient = _

  protected[jdbc] def tableName: String
  protected[jdbc] def columnPrefixName: String
  protected[jdbc] def dbUrl: String
  protected[jdbc] def dbUserName: String
  protected[jdbc] def dbPassword: String
  protected[jdbc] def dbName: String
  protected[jdbc] def BINARY_TYPE_NAME: String
  protected[jdbc] def INCREMENT_TYPE_NAME: String

  protected[this] def incrementColumn: String = s"${columnPrefixName}0"
  protected[this] def queryColumn: String     = s"${columnPrefixName}2"
  protected[this] def timestampColumn: String = s"${columnPrefixName}1"

  /**
    * This function for setting database JDBC jar file name.
    * from local upload to configurator server for connector worker container to download use.
    * @return JDBC driver file name
    */
  protected[jdbc] def jdbcDriverJarFileName: String

  private[this] var inputDataThread: Releasable = _

  private[this] def setupDatabase(inputDataTime: Long): LongAdder = {
    // Create database client
    client = DatabaseClient.builder.url(dbUrl).user(dbUserName).password(dbPassword).build

    // Create table
    val columns = Seq(
      RdbColumn(s"$timestampColumn", "TIMESTAMP", false),
      RdbColumn(s"$queryColumn", "VARCHAR(45)", false),
      RdbColumn(s"${columnPrefixName}3", "INTEGER", false),
      RdbColumn(s"${columnPrefixName}4", BINARY_TYPE_NAME, false)
    )

    client.createTable(tableName, Seq(RdbColumn(s"$incrementColumn", INCREMENT_TYPE_NAME, true)) ++ columns)
    val tableTotalCount = new LongAdder()

    inputDataThread = {
      val pool            = Executors.newSingleThreadExecutor()
      val startTime: Long = CommonUtils.current()
      pool.execute { () =>
        val sql =
          s"INSERT INTO $tableName(${columns.map(_.name).mkString(",")}) VALUES (${columns.map(_ => "?").mkString(",")})"
        val preparedStatement = client.connection.prepareStatement(sql)
        try {
          while ((CommonUtils.current() - startTime) <= inputDataTime) {
            // 432000000 is 5 days ago
            val timestampData = new Timestamp(CommonUtils.current() - 432000000 + tableTotalCount.intValue())
            preparedStatement.setTimestamp(1, timestampData)
            preparedStatement.setString(2, CommonUtils.randomString())
            preparedStatement.setInt(3, CommonUtils.randomInteger())
            preparedStatement.setBytes(4, s"binary-value${CommonUtils.randomInteger()}".getBytes)
            preparedStatement.execute()
            tableTotalCount.add(1)
          }
        } finally Releasable.close(preparedStatement)
      }
      () => {
        pool.shutdown()
        pool.awaitTermination(inputDataTime, TimeUnit.SECONDS)
      }
    }
    tableTotalCount
  }

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testNormal(platform: ContainerPlatform): Unit =
    close(platform.setup()) { resourceRef =>
      val startTestTimestamp                     = CommonUtils.current()
      val tableTotalCount                        = setupDatabase(inputDataTime)
      val (brokerClusterInfo, workerClusterInfo) = startCluster(resourceRef)
      val connectorKey                           = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
      val topicKey                               = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
      val connectorAdmin                         = ConnectorAdmin(workerClusterInfo)

      createConnector(connectorAdmin, connectorKey, topicKey)
      val consumer =
        Consumer
          .builder()
          .topicKey(topicKey)
          .offsetFromBegin()
          .connectionProps(brokerClusterInfo.connectionProps)
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build()
      await(() => CommonUtils.current() - startTestTimestamp >= inputDataTime && count() == tableTotalCount.intValue())

      val statement = client.connection.createStatement()
      try {
        // Check the topic data
        val result = consumer.poll(java.time.Duration.ofSeconds(60), tableTotalCount.intValue()).asScala
        tableTotalCount.intValue() shouldBe result.size

        val resultSet = statement.executeQuery(s"select * from $tableName order by $queryColumn")

        val tableData: Seq[String] =
          Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(queryColumn)).toSeq
        val topicData: Seq[String] = result
          .map(record => record.key.get.cell(queryColumn).value().toString)
          .sorted[String]
          .toSeq

        checkData(tableData, topicData)
      } finally {
        Releasable.close(statement)
        Releasable.close(consumer)
      }
    }(_ => ())

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testConnectorStartPauseResumeDelete(platform: ContainerPlatform): Unit =
    close(platform.setup()) { resourceRef =>
      val startTestTimestamp                     = CommonUtils.current()
      val tableTotalCount                        = setupDatabase(inputDataTime)
      val (brokerClusterInfo, workerClusterInfo) = startCluster(resourceRef)
      val connectorKey                           = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
      val topicKey                               = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
      val connectorAdmin                         = ConnectorAdmin(workerClusterInfo)
      createConnector(connectorAdmin, connectorKey, topicKey)

      val consumer =
        Consumer
          .builder()
          .topicKey(topicKey)
          .offsetFromBegin()
          .connectionProps(brokerClusterInfo.connectionProps)
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build()

      val statement = client.connection.createStatement()
      try {
        val result1 = consumer.poll(java.time.Duration.ofSeconds(5), tableTotalCount.intValue()).asScala
        tableTotalCount.intValue() >= result1.size shouldBe true

        result(connectorAdmin.pause(connectorKey))
        result(connectorAdmin.resume(connectorKey))
        TimeUnit.SECONDS.sleep(3)

        consumer.seekToBeginning()
        val result2 = consumer.poll(java.time.Duration.ofSeconds(5), tableTotalCount.intValue()).asScala
        result2.size >= result1.size shouldBe true

        result(connectorAdmin.delete(connectorKey))
        createConnector(connectorAdmin, connectorKey, topicKey)

        // Check the table and topic data size
        await(
          () => CommonUtils.current() - startTestTimestamp >= inputDataTime && count() == tableTotalCount.intValue()
        )

        consumer.seekToBeginning() //Reset consumer
        val result3 = consumer.poll(java.time.Duration.ofSeconds(60), tableTotalCount.intValue()).asScala
        tableTotalCount.intValue() shouldBe result3.size

        // Check the topic data is equals the database table
        val resultSet = statement.executeQuery(s"select * from $tableName order by $queryColumn")
        val tableData: Seq[String] =
          Iterator.continually(resultSet).takeWhile(_.next()).map(_.getString(queryColumn)).toSeq
        val topicData: Seq[String] = result3
          .map(record => record.key.get.cell(queryColumn).value().toString)
          .sorted[String]
          .toSeq
        checkData(tableData, topicData)
      } finally {
        Releasable.close(consumer)
        Releasable.close(statement)
      }
    }(_ => ())

  @ParameterizedTest(name = "{displayName} with {argumentsWithNames}")
  @MethodSource(value = Array("parameters"))
  def testTableInsertUpdateDelete(platform: ContainerPlatform): Unit =
    close(platform.setup()) { resourceRef =>
      val startTestTimestamp                     = CommonUtils.current()
      val tableTotalCount                        = setupDatabase(inputDataTime)
      val (brokerClusterInfo, workerClusterInfo) = startCluster(resourceRef)
      val connectorKey                           = ConnectorKey.of(CommonUtils.randomString(5), "JDBC-Source-Connector-Test")
      val topicKey                               = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
      val connectorAdmin                         = ConnectorAdmin(workerClusterInfo)
      createConnector(connectorAdmin, connectorKey, topicKey)

      val consumer =
        Consumer
          .builder()
          .topicKey(topicKey)
          .offsetFromBegin()
          .connectionProps(brokerClusterInfo.connectionProps)
          .keySerializer(Serializer.ROW)
          .valueSerializer(Serializer.BYTES)
          .build()

      val insertPreparedStatement =
        client.connection.prepareStatement(
          s"INSERT INTO $tableName($timestampColumn, $queryColumn) VALUES(?,?)"
        )
      val updatePreparedStatement =
        client.connection.prepareStatement(
          s"UPDATE $tableName SET $timestampColumn=? WHERE $queryColumn=?"
        )
      val deletePreparedStatement =
        client.connection.prepareStatement(
          s"DELETE FROM $tableName WHERE $timestampColumn=?"
        )
      val statement = client.connection.createStatement()
      try {
        val resultSet = statement.executeQuery(s"select * from $tableName order by $queryColumn")
        val queryResult: (Timestamp, String) = Iterator
          .continually(resultSet)
          .takeWhile(_.next())
          .map { x =>
            (x.getTimestamp(timestampColumn), x.getString(queryColumn))
          }
          .toSeq
          .head

        await(
          () => CommonUtils.current() - startTestTimestamp >= inputDataTime && count() == tableTotalCount.intValue()
        )
        insertPreparedStatement.setTimestamp(1, new Timestamp(queryResult._1.getTime + 86400000)) // 86400000 is a day
        insertPreparedStatement.setString(2, queryResult._2)
        insertPreparedStatement.executeUpdate()

        val expectTopicCount = tableTotalCount.intValue() + 1
        val result           = consumer.poll(java.time.Duration.ofSeconds(60), expectTopicCount).asScala
        expectTopicCount shouldBe result.size
        val topicData: Seq[String] = result
          .map(record => record.key.get.cell(queryColumn).value().toString)
          .sorted[String]
          .toSeq
        val tableResultSet = statement.executeQuery(s"select * from $tableName order by $queryColumn")
        val resultTableData: Seq[String] =
          Iterator.continually(tableResultSet).takeWhile(_.next()).map(_.getString(queryColumn)).toSeq
        checkData(resultTableData, topicData)

        deletePreparedStatement.setTimestamp(1, queryResult._1)
        deletePreparedStatement.executeUpdate()
        consumer.seekToBeginning()
        val deleteResult = consumer.poll(java.time.Duration.ofSeconds(30), expectTopicCount).asScala
        expectTopicCount shouldBe deleteResult.size

        // Test update data for the table
        updatePreparedStatement.setTimestamp(1, new Timestamp(queryResult._1.getTime + 172800000)) // 172800000 is 2 day
        updatePreparedStatement.setString(2, queryResult._2)
        updatePreparedStatement.executeUpdate()
        consumer.seekToBeginning()
        val updateResult = consumer.poll(java.time.Duration.ofSeconds(30), expectTopicCount).asScala
        expectTopicCount shouldBe updateResult.size
      } finally {
        Releasable.close(insertPreparedStatement)
        Releasable.close(updatePreparedStatement)
        Releasable.close(deletePreparedStatement)
        Releasable.close(statement)
        Releasable.close(consumer)
      }
    }(_ => ())

  private[this] def count(): Int = {
    val prepareStatement = client.connection.prepareStatement(s"SELECT count(*) from $tableName")
    try {
      val resultSet = prepareStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getInt(1)
        else 0
      } finally Releasable.close(resultSet)
    } finally Releasable.close(prepareStatement)
  }

  private[this] def startCluster(resourceRef: ResourceRef): (BrokerClusterInfo, WorkerClusterInfo) = {
    log.info("[ZOOKEEPER] start to test zookeeper")
    TimeUnit.SECONDS.sleep(5)

    val zkCluster = result(
      resourceRef.zookeeperApi.request
        .key(resourceRef.generateObjectKey)
        .nodeName(resourceRef.nodeNames.head)
        .create()
    )
    result(resourceRef.zookeeperApi.start(zkCluster.key))
    assertCluster(
      () => result(resourceRef.zookeeperApi.list()),
      () => result(resourceRef.containerApi.get(zkCluster.key).map(_.flatMap(_.containers))),
      zkCluster.key
    )

    log.info("[BROKER] start to test broker")
    val brokerClusterInfo = result(
      resourceRef.brokerApi.request
        .key(resourceRef.generateObjectKey)
        .zookeeperClusterKey(zkCluster.key)
        .nodeName(resourceRef.nodeNames.head)
        .create()
    )
    result(resourceRef.brokerApi.start(brokerClusterInfo.key))
    assertCluster(
      () => result(resourceRef.brokerApi.list()),
      () => result(resourceRef.containerApi.get(brokerClusterInfo.key).map(_.flatMap(_.containers))),
      brokerClusterInfo.key
    )

    log.info("[WORKER] create ...")
    val fileInfo = result(
      resourceRef.fileApi.request.file(new File(CommonUtils.path(jarFolderPath, jdbcDriverJarFileName))).upload()
    )

    val workerClusterInfo = result(
      resourceRef.workerApi.request
        .key(resourceRef.generateObjectKey)
        .brokerClusterKey(brokerClusterInfo.key)
        .nodeName(resourceRef.nodeNames.head)
        .sharedJarKeys(Set(fileInfo.key))
        .create()
    )
    log.info("[WORKER] create done")
    result(resourceRef.workerApi.start(workerClusterInfo.key))
    log.info("[WORKER] start done")
    assertCluster(
      () => result(resourceRef.workerApi.list()),
      () => result(resourceRef.containerApi.get(workerClusterInfo.key).map(_.flatMap(_.containers))),
      workerClusterInfo.key
    )
    testConnectors(workerClusterInfo)
    (brokerClusterInfo, workerClusterInfo)
  }

  protected[jdbc] def props: JDBCSourceConnectorConfig

  private[this] def createConnector(
    connectorAdmin: ConnectorAdmin,
    connectorKey: ConnectorKey,
    topicKey: TopicKey
  ): Unit =
    result(
      connectorAdmin
        .connectorCreator()
        .connectorKey(connectorKey)
        .connectorClass(classOf[JDBCSourceConnector])
        .topicKey(topicKey)
        .numberOfTasks(1)
        .settings(props.toMap)
        .create()
    )

  private[this] def testConnectors(cluster: WorkerClusterInfo): Unit =
    await(
      () =>
        try {
          log.info(s"worker node head: ${cluster.nodeNames.head}:${cluster.clientPort}")
          result(ConnectorAdmin(cluster).connectorDefinitions()).nonEmpty
        } catch {
          case e: Throwable =>
            log.info(s"[WORKER] worker cluster:${cluster.name} is starting ... retry", e)
            false
        }
    )

  private[this] def checkData(tableData: Seq[String], topicData: Seq[String]): Unit =
    tableData.foreach { data =>
      topicData.contains(data) shouldBe true
    }

  @AfterEach
  def afterTest(): Unit = {
    Releasable.close(inputDataThread)
    if (client != null) {
      val statement: Statement = client.connection.createStatement()
      statement.execute(s"drop table $tableName")
    }
    Releasable.close(client)
  }
}

object BasicTestConnectorCollie {
  def parameters: java.util.stream.Stream[Arguments] = ContainerPlatform.all.map(o => Arguments.of(o)).asJava.stream()
}
