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
import akka.http.scaladsl.model.{ContentTypes, _}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{as, complete, entity, path, pathPrefix, put, _}
import oharastream.ohara.agent.WorkerCollie
import oharastream.ohara.client.configurator.ConnectorApi
import oharastream.ohara.client.configurator.ConnectorApi.Creation
import oharastream.ohara.client.configurator.ValidationApi._
import oharastream.ohara.configurator.store.DataStore

import scala.concurrent.ExecutionContext

private[configurator] object ValidationRoute {
  def apply(
    implicit dataStore: DataStore,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): server.Route =
    pathPrefix(VALIDATION_KIND) {
      path(ConnectorApi.PREFIX | ConnectorApi.KIND) {
        put {
          entity(as[Creation])(
            req =>
              complete(
                connectorAdmin(req.workerClusterKey) { (_, connectorAdmin) =>
                  connectorAdmin
                    .connectorValidator()
                    .settings(req.plain)
                    .className(req.className)
                    // the topic name is composed by group and name. However, the kafka topic is still a pure string.
                    // Hence, we can't just push Ohara topic "key" to kafka topic "name".
                    // The name of topic is a required for connector and hence we have to fill the filed when starting
                    // connector.
                    .topicKeys(req.topicKeys)
                    // add the connector key manually since the arguments exposed to user is "group" and "name" than "key"
                    .connectorKey(req.key)
                    .run()
                }.map(settingInfo => HttpEntity(ContentTypes.`application/json`, settingInfo.toJsonString))
              )
          )
        }
      }
    }
}
