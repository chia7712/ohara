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

package oharastream.ohara.agent

import oharastream.ohara.client.configurator.ClusterState
import oharastream.ohara.client.configurator.ContainerApi.ContainerInfo
import oharastream.ohara.common.setting.ObjectKey

/**
  * the information of running cluster. Those information may be changed over time.
  * Noted: all collies take this class to be the basic data type, and we should NOT extend it for different type since
  *        we should NOT complicate our collie and collie should be focused on the "operation" rather than collect data.
  */
case class ClusterStatus(
  group: String,
  name: String,
  kind: ClusterKind,
  state: Option[ClusterState],
  error: Option[String],
  containers: Seq[ContainerInfo]
) {
  /**
    * a helper method used to generate the key of this data.
    * @return key
    */
  def key: ObjectKey = ObjectKey.of(group, name)

  final def nodeNames: Set[String] = containers.map(_.nodeName).toSet

  final def aliveContainers: Seq[ContainerInfo] =
    containers
    // Currently, docker naming rule for "Running" and Kubernetes naming rule for "PENDING"
    // it is ok that we use the containerState.RUNNING or containerState.PENDING here.
      .filter(container => container.state.toUpperCase == "RUNNING" || container.state.toUpperCase == "PENDING")

  /**
    * the nodes do run the containers of cluster.
    * @return a collection of node names
    */
  final def aliveNodes: Set[String] = aliveContainers.map(_.nodeName).toSet
}
