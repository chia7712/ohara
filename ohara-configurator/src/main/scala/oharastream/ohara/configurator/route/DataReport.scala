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

package oharastream.ohara.configurator.route

import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.FileInfoApi.FileInfo
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.client.configurator.ObjectApi.ObjectInfo
import oharastream.ohara.client.configurator.PipelineApi.Pipeline
import oharastream.ohara.client.configurator.ShabondiApi.ShabondiClusterInfo
import oharastream.ohara.client.configurator.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.TopicApi.TopicInfo
import oharastream.ohara.client.configurator.VolumeApi.Volume
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo

case class DataReport(
  topicInfos: Map[TopicInfo, DataCondition],
  volumes: Map[Volume, DataCondition],
  connectorInfos: Map[ConnectorInfo, DataCondition],
  fileInfos: Seq[FileInfo],
  nodes: Seq[Node],
  objectInfos: Seq[ObjectInfo],
  pipelines: Seq[Pipeline],
  zookeeperClusterInfos: Map[ZookeeperClusterInfo, DataCondition],
  brokerClusterInfos: Map[BrokerClusterInfo, DataCondition],
  workerClusterInfos: Map[WorkerClusterInfo, DataCondition],
  streamClusterInfos: Map[StreamClusterInfo, DataCondition],
  shabondiClusterInfos: Map[ShabondiClusterInfo, DataCondition]
) {
  def runningTopics: Seq[TopicInfo]         = topicInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningConnectors: Seq[ConnectorInfo] = connectorInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningZookeepers: Seq[ZookeeperClusterInfo] =
    zookeeperClusterInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningBrokers: Seq[BrokerClusterInfo]     = brokerClusterInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningWorkers: Seq[WorkerClusterInfo]     = workerClusterInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningStreams: Seq[StreamClusterInfo]     = streamClusterInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningShabondis: Seq[ShabondiClusterInfo] = shabondiClusterInfos.filter(_._2 == DataCondition.RUNNING).keys.toSeq
  def runningVolumes: Seq[Volume]                = volumes.filter(_._2 == DataCondition.RUNNING).keys.toSeq
}
