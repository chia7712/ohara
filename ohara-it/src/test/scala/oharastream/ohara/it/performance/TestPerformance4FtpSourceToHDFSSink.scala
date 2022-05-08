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
import oharastream.ohara.connector.ftp.FtpSource
import oharastream.ohara.connector.hdfs.sink.HDFSSink
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import spray.json.{JsNumber, JsString}

@EnabledIfEnvironmentVariable(named = "ohara.it.performance.hdfs.url", matches = ".*")
class TestPerformance4FtpSourceToHDFSSink extends BasicTestPerformance4Ftp {
  private[this] val HDFS_URL_KEY: String = "ohara.it.performance.hdfs.url"
  private[this] val ftpCompletedPath     = "/completed"
  private[this] val ftpErrorPath         = "/error"
  private[this] val (path, _, _)         = setupInputData(timeoutOfInputData)

  @Test
  def test(): Unit = {
    val ftp  = ftpClient()
    val hdfs = hdfsClient()
    try {
      createTopic()
      loopInputDataThread(setupInputData)
      //Running FTP Source Connector
      setupConnector(
        connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
        className = classOf[FtpSource].getName,
        settings = ftpSettings
          + (CsvConnectorDefinitions.INPUT_FOLDER_KEY -> JsString(path))
          + (CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> JsString(
            PerformanceTestingUtils.createFolder(ftp, ftpCompletedPath)
          ))
          + (CsvConnectorDefinitions.ERROR_FOLDER_KEY -> JsString(
            PerformanceTestingUtils.createFolder(ftp, ftpErrorPath)
          ))
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
    } finally {
      Releasable.close(hdfs)
      Releasable.close(ftp)
    }
  }

  override protected def afterStoppingConnectors(
    connectorInfos: Seq[ConnectorInfo],
    topicInfos: Seq[TopicInfo]
  ): Unit = {
    if (cleanupTestData) {
      //Delete file for the FTP
      val ftp  = ftpClient()
      val hdfs = hdfsClient()
      try {
        PerformanceTestingUtils.deleteFolder(ftp, path)
        PerformanceTestingUtils.deleteFolder(ftp, ftpCompletedPath)
        PerformanceTestingUtils.deleteFolder(ftp, ftpErrorPath)

        //Delete file from the HDFS
        topicInfos.foreach { topicInfo =>
          val path = s"${PerformanceTestingUtils.dataDir}/${topicInfo.topicNameOnKafka}"
          PerformanceTestingUtils.deleteFolder(hdfs, path)
        }
      } finally {
        Releasable.close(hdfs)
        Releasable.close(ftp)
      }
    }
  }

  private[this] def hdfsClient(): FileSystem =
    FileSystem.hdfsBuilder.url(sys.env(HDFS_URL_KEY)).build
}
