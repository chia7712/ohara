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

package oharastream.ohara.it.performance

import java.io.File
import java.sql.Timestamp
import java.util.concurrent.atomic.LongAdder

import oharastream.ohara.client.configurator.FileInfoApi
import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.Row
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

@EnabledIfEnvironmentVariable(named = "ohara.it.performance.jdbc.url", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.jdbc.username", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.jdbc.password", matches = ".*")
private[performance] abstract class BasicTestPerformance4Jdbc extends BasicTestPerformance {
  private[this] val DB_URL_KEY: String       = "ohara.it.performance.jdbc.url"
  private[this] val DB_USER_NAME_KEY: String = "ohara.it.performance.jdbc.username"
  private[this] val DB_PASSWORD_KEY: String  = "ohara.it.performance.jdbc.password"
  protected[this] val url: String            = sys.env(DB_URL_KEY)
  protected[this] val user: String           = sys.env(DB_USER_NAME_KEY)
  protected[this] val password: String       = sys.env(DB_PASSWORD_KEY)
  private[this] val jarFolderPath: String    = sys.env.getOrElse("ohara.it.jar.folder", "/jar")

  private[this] val NEED_DELETE_DATA_KEY: String  = PerformanceTestingUtils.DATA_CLEANUP_KEY
  protected[this] val needDeleteData: Boolean     = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "false").toBoolean
  protected[this] val timestampColumnName: String = "COLUMN0"

  protected def tableName: String
  protected def isColumnNameUpperCase: Boolean = true
  protected[this] var client: DatabaseClient   = _

  private[this] val columnNames: Seq[String] = Seq(timestampColumnName) ++ rowData().cells().asScala.map(_.name)
  private[this] val columnInfos = columnNames
    .map(columnName => if (!isColumnNameUpperCase) columnName.toLowerCase else columnName.toUpperCase)
    .zipWithIndex
    .map {
      case (columnName, index) =>
        if (index == 0) RdbColumn(columnName, "TIMESTAMP", true)
        else if (index == 1) RdbColumn(columnName, "VARCHAR(45)", true)
        else RdbColumn(columnName, "VARCHAR(45)", false)
    }

  @BeforeEach
  final def setup(): Unit = {
    client = DatabaseClient.builder.url(url).user(user).password(password).build
  }

  override protected def sharedJars: Set[ObjectKey] = {
    val jarApi: FileInfoApi.Access = FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
    val localFiles                 = new File(jarFolderPath)
    localFiles.list
      .map(fileName => {
        val jar = new File(CommonUtils.path(jarFolderPath, fileName))
        result(jarApi.request.file(jar).upload()).key
      })
      .toSet
  }

  protected[this] def createTable(): Unit = {
    log.info(s"Create the ${tableName} table for JDBC source connector test")
    client.createTable(tableName, columnInfos)
  }

  protected[this] def setupInputData(timeout: Duration): (String, Long, Long) = {
    val client = DatabaseClient.builder.url(url).user(user).password(password).build

    try {
      // 432000000 is 5 days ago
      val timestampData = new Timestamp(CommonUtils.current() - 432000000)
      val sql = s"INSERT INTO $tableName VALUES " + columnInfos
        .map(_ => "?")
        .mkString("(", ",", ")")

      val result = generateData(
        numberOfRowsToFlush,
        timeout,
        (rows: Seq[Row]) => {
          val preparedStatement = client.connection.prepareStatement(sql)
          try {
            val count       = new LongAdder()
            val sizeInBytes = new LongAdder()
            preparedStatement.setTimestamp(1, timestampData)
            rows.foreach(row => {
              row.asScala.zipWithIndex.foreach {
                case (result, index) => {
                  val value = result.value().toString()
                  sizeInBytes.add(value.length)
                  preparedStatement.setString(index + 2, value)
                }
              }
              preparedStatement.addBatch()
              count.increment()
            })
            preparedStatement.executeBatch()
            (count.longValue(), sizeInBytes.longValue())
          } finally Releasable.close(preparedStatement)
        }
      )
      (tableName, result._1, result._2)
    } finally Releasable.close(client)
  }

  @AfterEach
  def close(): Unit = Releasable.close(client)
}
