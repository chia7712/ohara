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
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.connector.perf.PerfSource
import org.junit.jupiter.api.Test

class TestPerformance4PerfSource extends BasicTestPerformance {
  @Test
  def test(): Unit = {
    createTopic()
    setupConnector(
      connectorKey = ConnectorKey.of(groupName, CommonUtils.randomString(5)),
      className = classOf[PerfSource].getName,
      settings = Map.empty
    )
    sleepUntilEnd()
  }

  override protected def afterStoppingConnectors(
    connectorInfos: Seq[ConnectorApi.ConnectorInfo],
    topicInfos: Seq[TopicApi.TopicInfo]
  ): Unit = {}
}
