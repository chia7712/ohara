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

import oharastream.ohara.client.configurator.{ConnectorApi, TopicApi}
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.ftp.FtpSource
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.jupiter.api.Test
import spray.json.{JsNumber, JsString}

class TestPerformance4FtpSource extends BasicTestPerformance4Ftp {
  @Test
  def test(): Unit = {
    val ftp = ftpClient()
    try {
      createTopic()
      val completedPath = "/completed"
      val errorPath     = "/error"
      val (path, _, _)  = setupInputData(timeoutOfInputData)
      try {
        loopInputDataThread(setupInputData)
        setupConnector(
          connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
          className = classOf[FtpSource].getName,
          settings = ftpSettings
            + (CsvConnectorDefinitions.INPUT_FOLDER_KEY -> JsString(path))
            + (CsvConnectorDefinitions.COMPLETED_FOLDER_KEY -> JsString(
              PerformanceTestingUtils.createFolder(ftp, completedPath)
            ))
            + (CsvConnectorDefinitions.ERROR_FOLDER_KEY -> JsString(
              PerformanceTestingUtils.createFolder(ftp, errorPath)
            ))
            + (CsvConnectorDefinitions.SIZE_OF_FILE_CACHE_KEY -> JsNumber(fileNameCacheSize))
        )
        sleepUntilEnd()
      } finally if (cleanupTestData) {
        PerformanceTestingUtils.deleteFolder(ftp, path)
        PerformanceTestingUtils.deleteFolder(ftp, completedPath)
        PerformanceTestingUtils.deleteFolder(ftp, errorPath)
      }
    } finally Releasable.close(ftp)
  }

  override protected def afterStoppingConnectors(
    connectorInfos: Seq[ConnectorApi.ConnectorInfo],
    topicInfos: Seq[TopicApi.TopicInfo]
  ): Unit = {}
}
