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
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import oharastream.ohara.agent.ServiceCollie
import oharastream.ohara.client.configurator.ContainerApi
import oharastream.ohara.client.configurator.ContainerApi._
import oharastream.ohara.common.setting.ObjectKey
import spray.json.DefaultJsonProtocol._

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

object ContainerRoute {
  @nowarn("cat=deprecation")
  def apply(implicit serviceCollie: ServiceCollie, executionContext: ExecutionContext): server.Route =
    path((ContainerApi.CONTAINER_PREFIX_PATH | ContainerApi.KIND) / Segment)({ clusterName =>
      parameter(GROUP_KEY ? GROUP_DEFAULT) { group =>
        get {
          complete(
            serviceCollie
              .clusters()
              .map(_.filter(_.key == ObjectKey.of(group, clusterName)).map { cluster =>
                ContainerGroup(
                  clusterKey = ObjectKey.of(group, clusterName),
                  clusterType = cluster.kind.toString.toLowerCase,
                  containers = cluster.containers
                )
              })
          )
        }
      }
    })
}
