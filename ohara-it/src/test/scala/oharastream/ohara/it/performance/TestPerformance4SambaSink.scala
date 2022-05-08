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
import oharastream.ohara.connector.smb.SmbSink
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.jupiter.api.Test
import spray.json.{JsNumber, JsString}

class TestPerformance4SambaSink extends BasicTestPerformance4Samba {
  private[this] val outputDir: String = "output"

  @Test
  def test(): Unit = {
    val samba = sambaClient()
    try {
      createTopic()
      produce(timeoutOfInputData)
      loopInputDataThread(produce)
      setupConnector(
        connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
        className = classOf[SmbSink].getName,
        settings = sambaSettings
          ++ Map(
            CsvConnectorDefinitions.OUTPUT_FOLDER_KEY -> JsString(
              PerformanceTestingUtils.createFolder(samba, outputDir)
            ),
            CsvConnectorDefinitions.FLUSH_SIZE_KEY -> JsNumber(numberOfCsvFileToFlush)
          )
      )
      sleepUntilEnd()
    } finally Releasable.close(samba)
  }

  override protected def afterStoppingConnectors(connectorInfos: Seq[ConnectorInfo], topicInfos: Seq[TopicInfo]): Unit =
    if (needDeleteData)
      topicInfos.foreach { topicInfo =>
        val path  = s"$outputDir/${topicInfo.topicNameOnKafka}"
        val samba = sambaClient()
        try {
          if (PerformanceTestingUtils.exists(samba, path)) PerformanceTestingUtils.deleteFolder(samba, path)
        } finally Releasable.close(samba)
      }
}
