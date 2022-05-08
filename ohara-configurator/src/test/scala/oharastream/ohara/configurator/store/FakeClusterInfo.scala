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

package oharastream.ohara.configurator.store

import oharastream.ohara.client.configurator.{ClusterInfo, ClusterState}
import oharastream.ohara.common.util.CommonUtils
import spray.json.JsValue

case class FakeClusterInfo(override val name: String) extends ClusterInfo {
  override def ports: Set[Int] = Set.empty

  override def nodeNames: Set[String] = Set.empty

  override def aliveNodes: Set[String] = Set.empty

  override def group: String = "fake_group"

  override def lastModified: Long = CommonUtils.current()

  override def kind: String = "fake_cluster"

  override def tags: Map[String, JsValue] = Map.empty

  override def state: Option[ClusterState] = None

  override def error: Option[String] = None

  override def settings: Map[String, JsValue] = throw new UnsupportedOperationException

  override def raw: Map[String, JsValue] = Map.empty
}
