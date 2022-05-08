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
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.ftp.FtpSink
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.jupiter.api.Test
import spray.json.{JsNumber, JsString}

class TestPerformance4FtpSink extends BasicTestPerformance4Ftp {
  private[this] val dataDir: String = "/tmp"

  @Test
  def test(): Unit = {
    val ftp = ftpClient()
    try {
      createTopic()
      produce(timeoutOfInputData)
      loopInputDataThread(produce)
      setupConnector(
        connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
        className = classOf[FtpSink].getName(),
        settings = ftpSettings
          ++ Map(
            CsvConnectorDefinitions.OUTPUT_FOLDER_KEY -> JsString(PerformanceTestingUtils.createFolder(ftp, dataDir)),
            CsvConnectorDefinitions.FLUSH_SIZE_KEY    -> JsNumber(numberOfCsvFileToFlush)
          )
      )
      sleepUntilEnd()
    } finally Releasable.close(ftp)
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (cleanupTestData)
      topicInfos.foreach { topicInfo =>
        val path = s"${dataDir}/${topicInfo.topicNameOnKafka}"
        val ftp  = ftpClient()
        try if (PerformanceTestingUtils.exists(ftp, path)) PerformanceTestingUtils.deleteFolder(ftp, path)
        finally Releasable.close(ftp)
      }
}
