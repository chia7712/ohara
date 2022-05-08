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

package oharastream.ohara.client.configurator

import oharastream.ohara.client.Enum
import spray.json.{JsString, JsValue, RootJsonFormat}

import spray.json.DefaultJsonProtocol._

/**
  * This class is a top level of cluster "status". We define the cluster lifecycle here
  * and use it to represent the cluster state.
  * The name is a part of "Restful APIs" so "DON'T" change it arbitrarily
  * Noted:
  * 1) you should not assume these states as containers state. A cluster failed doesn't mean container failed.
  * 2) ClusterState is a part of ClusterInfo so it must be serializable to be stored by Configurator
  */
abstract sealed class ClusterState(val name: String) extends Serializable
object ClusterState extends Enum[ClusterState] {
  implicit val STATE_FORMAT: RootJsonFormat[ClusterState] = new RootJsonFormat[ClusterState] {
    override def read(json: JsValue): ClusterState = ClusterState.forName(json.convertTo[String].toUpperCase)
    override def write(obj: ClusterState): JsValue = JsString(obj.name)
  }

  /**
    * the service is pending to setup.
    */
  case object PENDING extends ClusterState("PENDING")

  /**
    * the service is running. Noted that it does not imply the all containers are running
    */
  case object RUNNING extends ClusterState("RUNNING")

  /**
    * this service is unable to work
    */
  case object FAILED extends ClusterState("FAILED")

  /**
    * the other state.
    */
  case object UNKNOWN extends ClusterState("UNKNOWN")
}
