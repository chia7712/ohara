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

object LogApi {
  val KIND: String = "log"

  @deprecated(message = s"replaced by $KIND", since = "0.11.0")
  val LOG_PREFIX_PATH: String = "logs"

  val SINCE_SECONDS_KEY: String = "sinceSeconds"

  case class NodeLog(hostname: String, value: String)
  implicit val NODE_LOG_FORMAT: RootJsonFormat[NodeLog] = jsonFormat2(NodeLog)

  case class ClusterLog(clusterKey: ObjectKey, logs: Seq[NodeLog])
  implicit val CLUSTER_LOG_FORMAT: RootJsonFormat[ClusterLog] = jsonFormat2(ClusterLog)

  class Access extends BasicAccess(KIND) {
    private[this] def _url(service: String, clusterKey: ObjectKey): String =
      s"$url/$service/${clusterKey.name()}?$GROUP_KEY=${clusterKey.group()}"

    def log4ZookeeperCluster(clusterKey: ObjectKey)(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](_url(ZookeeperApi.PREFIX, clusterKey))

    def log4ZookeeperCluster(clusterKey: ObjectKey, sinceSeconds: Long)(
      implicit executionContext: ExecutionContext
    ): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](
        urlBuilder
          .key(clusterKey)
          .prefix(ZookeeperApi.PREFIX)
          .param(SINCE_SECONDS_KEY, sinceSeconds.toString)
          .build()
      )

    def log4BrokerCluster(clusterKey: ObjectKey)(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](_url(BrokerApi.PREFIX, clusterKey))

    def log4BrokerCluster(clusterKey: ObjectKey, sinceSeconds: Long)(
      implicit executionContext: ExecutionContext
    ): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](
        urlBuilder
          .key(clusterKey)
          .prefix(BrokerApi.PREFIX)
          .param(SINCE_SECONDS_KEY, sinceSeconds.toString)
          .build()
      )

    def log4WorkerCluster(clusterKey: ObjectKey)(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](_url(WorkerApi.PREFIX, clusterKey))

    def log4WorkerCluster(clusterKey: ObjectKey, sinceSeconds: Long)(
      implicit executionContext: ExecutionContext
    ): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](
        urlBuilder
          .key(clusterKey)
          .prefix(WorkerApi.PREFIX)
          .param(SINCE_SECONDS_KEY, sinceSeconds.toString)
          .build()
      )

    def log4StreamCluster(clusterKey: ObjectKey)(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](_url(StreamApi.PREFIX, clusterKey))

    def log4StreamCluster(clusterKey: ObjectKey, sinceSeconds: Long)(
      implicit executionContext: ExecutionContext
    ): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](
        urlBuilder
          .key(clusterKey)
          .prefix(StreamApi.PREFIX)
          .param(SINCE_SECONDS_KEY, sinceSeconds.toString)
          .build()
      )

    def log4ShabondiCluster(clusterKey: ObjectKey)(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](_url(ShabondiApi.PREFIX, clusterKey))

    def log4ShabondiCluster(clusterKey: ObjectKey, sinceSeconds: Long)(
      implicit executionContext: ExecutionContext
    ): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](
        urlBuilder
          .key(clusterKey)
          .prefix(ShabondiApi.PREFIX)
          .param(SINCE_SECONDS_KEY, sinceSeconds.toString)
          .build()
      )

    def log4Configurator()(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](s"$url/$CONFIGURATOR_KIND")

    def log4Configurator(sinceSeconds: Long)(implicit executionContext: ExecutionContext): Future[ClusterLog] =
      exec.get[ClusterLog, ErrorApi.Error](
        urlBuilder.prefix(CONFIGURATOR_KIND).param(SINCE_SECONDS_KEY, sinceSeconds.toString).build()
      )
  }

  def access: Access = new Access
}
