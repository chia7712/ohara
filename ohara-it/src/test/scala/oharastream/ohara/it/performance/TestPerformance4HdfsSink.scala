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
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import spray.json.{JsNumber, JsString}

@EnabledIfEnvironmentVariable(named = "ohara.it.performance.hdfs.url", matches = ".*")
class TestPerformance4HdfsSink extends BasicTestPerformance {
  private[this] val HDFS_URL_KEY: String         = "ohara.it.performance.hdfs.url"
  private[this] val NEED_DELETE_DATA_KEY: String = PerformanceTestingUtils.DATA_CLEANUP_KEY
  private[this] val needDeleteData: Boolean      = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  @Test
  def test(): Unit = {
    val hdfs = hdfsClient()
    try {
      createTopic()
      produce(timeoutOfInputData)
      loopInputDataThread(produce)
      setupConnector(
        connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
        className = classOf[HDFSSink].getName(),
        settings = Map(
          CsvConnectorDefinitions.FLUSH_SIZE_KEY             -> JsNumber(numberOfCsvFileToFlush),
          oharastream.ohara.connector.hdfs.sink.HDFS_URL_KEY -> JsString(sys.env(HDFS_URL_KEY)),
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
      //Delete file from the HDFS
      val hdfs = hdfsClient()
      try topicInfos.foreach { topicInfo =>
        val path = s"${PerformanceTestingUtils.dataDir}/${topicInfo.topicNameOnKafka}"
        PerformanceTestingUtils.deleteFolder(hdfs, path)
      } finally Releasable.close(hdfs)
    }
  }

  private[this] def hdfsClient(): FileSystem = FileSystem.hdfsBuilder.url(sys.env(HDFS_URL_KEY)).build
}
