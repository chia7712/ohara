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
import oharastream.ohara.common.setting.ObjectKey
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

object ContainerApi {
  val KIND: String = "container"

  @deprecated(message = s"replaced by $KIND", since = "0.11.0")
  val CONTAINER_PREFIX_PATH: String = "containers"

  final case class PortMapping(hostIp: String, hostPort: Int, containerPort: Int)
  implicit val PORT_MAPPING_FORMAT: RootJsonFormat[PortMapping] = jsonFormat3(PortMapping)

  final case class ContainerInfo(
    nodeName: String,
    id: String,
    imageName: String,
    state: String,
    kind: String,
    name: String,
    size: Long,
    portMappings: Seq[PortMapping],
    environments: Map[String, String],
    hostname: String
  )

  implicit val CONTAINER_INFO_FORMAT: RootJsonFormat[ContainerInfo] = jsonFormat10(ContainerInfo)

  final case class ContainerGroup(clusterKey: ObjectKey, clusterType: String, containers: Seq[ContainerInfo])
  implicit val CONTAINER_GROUP_FORMAT: RootJsonFormat[ContainerGroup] = jsonFormat3(ContainerGroup)

  class Access private[configurator] extends BasicAccess(KIND) {
    def get(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[Seq[ContainerGroup]] =
      exec.get[Seq[ContainerGroup], ErrorApi.Error](urlBuilder.key(key).build())
  }

  def access: Access = new Access
}
