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

import oharastream.ohara.client.configurator.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.TopicApi.TopicInfo
import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.hdfs.sink.HDFSSink
import oharastream.ohara.connector.jdbc.source.JDBCSourceConnector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import spray.json.{JsNumber, JsString}

@EnabledIfEnvironmentVariable(named = "ohara.it.performance.hdfs.url", matches = ".*")
class TestPerformance4JDBCSourceToHDFSSink extends BasicTestPerformance4Jdbc {
  private[this] val HDFS_URL_KEY: String   = "ohara.it.performance.hdfs.url"
  override protected val tableName: String = s"TABLE${CommonUtils.randomString().toUpperCase()}"

  @Test
  def test(): Unit = {
    val hdfs = hdfsClient()
    try {
      createTable()
      setupInputData(timeoutOfInputData)
      loopInputDataThread(setupInputData)
      createTopic()

      //Running JDBC Source Connector
      setupConnector(
        connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
        className = classOf[JDBCSourceConnector].getName,
        settings = Map(
          oharastream.ohara.connector.jdbc.source.DB_URL_KEY                -> JsString(url),
          oharastream.ohara.connector.jdbc.source.DB_USERNAME_KEY           -> JsString(user),
          oharastream.ohara.connector.jdbc.source.DB_PASSWORD_KEY           -> JsString(password),
          oharastream.ohara.connector.jdbc.source.DB_TABLENAME_KEY          -> JsString(tableName),
          oharastream.ohara.connector.jdbc.source.TIMESTAMP_COLUMN_NAME_KEY -> JsString(timestampColumnName),
          oharastream.ohara.connector.jdbc.source.DB_SCHEMA_PATTERN_KEY     -> JsString(user),
          oharastream.ohara.connector.jdbc.source.FETCH_DATA_SIZE_KEY       -> JsNumber(10000),
          oharastream.ohara.connector.jdbc.source.FLUSH_DATA_SIZE_KEY       -> JsNumber(10000)
        )
      )

      //Running HDFS Sink Connector
      setupConnector(
        connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
        className = classOf[HDFSSink].getName,
        settings = Map(
          oharastream.ohara.connector.hdfs.sink.HDFS_URL_KEY   -> JsString(sys.env(HDFS_URL_KEY)),
          oharastream.ohara.connector.hdfs.sink.FLUSH_SIZE_KEY -> JsNumber(numberOfCsvFileToFlush),
          oharastream.ohara.connector.hdfs.sink.OUTPUT_FOLDER_KEY -> JsString(
            PerformanceTestingUtils.createFolder(hdfs, PerformanceTestingUtils.dataDir)
          )
        )
      )
      sleepUntilEnd()
    } finally Releasable.close(hdfs)
  }

  override protected def afterStoppingConnectors(
    connectorInfos: Seq[ConnectorInfo],
    topicInfos: Seq[TopicInfo]
  ): Unit = {
    if (needDeleteData) {
      //Drop table for the database
      client.dropTable(tableName)

      //Delete file from the HDFS
      val hdfs = hdfsClient()
      try {
        topicInfos.foreach { topicInfo =>
          val path = s"${PerformanceTestingUtils.dataDir}/${topicInfo.topicNameOnKafka}"
          PerformanceTestingUtils.deleteFolder(hdfs, path)
        }
      } finally Releasable.close(hdfs)
    }
  }

  private[this] def hdfsClient(): FileSystem =
    FileSystem.hdfsBuilder.url(sys.env(HDFS_URL_KEY)).build
}
