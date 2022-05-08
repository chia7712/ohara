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

import akka.http.scaladsl.server
import com.typesafe.scalalogging.Logger
import oharastream.ohara.agent.{ClusterKind, ServiceCollie}
import oharastream.ohara.client.configurator.NodeApi._
import oharastream.ohara.client.configurator.{ClusterInfo, NodeApi}
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.AdvertisedInfo
import oharastream.ohara.configurator.route.hook._
import oharastream.ohara.configurator.store.DataStore
import spray.json.DeserializationException

import scala.concurrent.{ExecutionContext, Future}
object NodeRoute {
  private[this] lazy val LOG = Logger(NodeRoute.getClass)

  private[this] def updateServices(
    nodes: Seq[Node]
  )(
    implicit advertisedInfo: AdvertisedInfo,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): Future[Seq[Node]] =
    serviceCollie
      .clusters()
      .map { clusters =>
        nodes.map { node =>
          node.copy(
            services = Seq(
              NodeService(
                name = NodeApi.ZOOKEEPER_SERVICE_NAME,
                clusterKeys = clusters
                  .filter(_.kind == ClusterKind.ZOOKEEPER)
                  .filter(_.aliveNodes.contains(node.name))
                  .map(_.key)
              ),
              NodeService(
                name = NodeApi.BROKER_SERVICE_NAME,
                clusterKeys = clusters
                  .filter(_.kind == ClusterKind.BROKER)
                  .filter(_.aliveNodes.contains(node.name))
                  .map(_.key)
              ),
              NodeService(
                name = NodeApi.WORKER_SERVICE_NAME,
                clusterKeys = clusters
                  .filter(_.kind == ClusterKind.WORKER)
                  .filter(_.aliveNodes.contains(node.name))
                  .map(_.key)
              ),
              NodeService(
                name = NodeApi.STREAM_SERVICE_NAME,
                clusterKeys = clusters
                  .filter(_.kind == ClusterKind.STREAM)
                  .filter(_.aliveNodes.contains(node.name))
                  .map(_.key)
              )
            ) ++ (if (advertisedInfo.hostname == node.hostname)
                    Seq(
                      NodeService(
                        name = NodeApi.CONFIGURATOR_SERVICE_NAME,
                        clusterKeys = Seq(ObjectKey.of("N/A", CommonUtils.hostname()))
                      )
                    )
                  else Seq.empty)
          )
        }
      }
      .recover {
        case e: Throwable =>
          LOG.error("failed to seek cluster information", e)
          nodes
      }

  /**
    * fetch the hardware resources.
    */
  private[this] def updateResources(
    nodes: Seq[Node]
  )(implicit serviceCollie: ServiceCollie, executionContext: ExecutionContext): Future[Seq[Node]] =
    serviceCollie
      .resources()
      .map(
        rs =>
          nodes.map { node =>
            node.copy(
              resources = rs.getOrElse(
                node.hostname,
                // Both cpu and memory are required by UI so we fake both ...
                Seq(Resource.cpu(0, None), Resource.memory(0, None))
              )
            )
          }
      )

  private[this] def verify(
    nodes: Seq[Node]
  )(implicit serviceCollie: ServiceCollie, executionContext: ExecutionContext): Future[Seq[Node]] =
    Future.traverse(nodes)(
      node =>
        serviceCollie
          .verifyNode(node)
          .map(
            _ =>
              node.copy(
                state = State.AVAILABLE,
                error = None
              )
          )
          .recover {
            case e: Throwable =>
              node.copy(
                state = State.UNAVAILABLE,
                error = Some(e.getMessage)
              )
          }
    )

  private[this] def updateRuntimeInfo(
    node: Node
  )(
    implicit
    advertisedInfo: AdvertisedInfo,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): Future[Node] =
    updateRuntimeInfo(Seq(node)).map(_.head)

  private[this] def updateRuntimeInfo(
    nodes: Seq[Node]
  )(
    implicit
    advertisedInfo: AdvertisedInfo,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): Future[Seq[Node]] =
    updateServices(nodes).flatMap(updateResources).flatMap(verify)

  private[this] def hookOfGet(
    implicit serviceCollie: ServiceCollie,
    advertisedInfo: AdvertisedInfo,
    executionContext: ExecutionContext
  ): HookOfGet[Node] = updateRuntimeInfo

  private[this] def hookOfList(
    implicit serviceCollie: ServiceCollie,
    advertisedInfo: AdvertisedInfo,
    executionContext: ExecutionContext
  ): HookOfList[Node] = updateRuntimeInfo

  private[this] def creationToNode(
    creation: Creation
  ): Future[Node] =
    // we don't update the run-time information since the node does not exist in store
    // so our collie CAN'T see them and fail to return any status for them.
    Future.successful(
      Node(
        hostname = creation.hostname,
        port = creation.port,
        user = creation.user,
        password = creation.password,
        services = Seq.empty,
        state = State.UNAVAILABLE,
        error = None,
        lastModified = CommonUtils.current(),
        resources = Seq.empty,
        tags = creation.tags
      )
    )

  private[this] def hookOfCreation: HookOfCreation[Creation, Node] = creationToNode(_)

  private[this] def hookAfterCreation(
    implicit advertisedInfo: AdvertisedInfo,
    executionContext: ExecutionContext,
    serviceCollie: ServiceCollie
  ): HookAfterCreation[Node] = updateRuntimeInfo(_)

  private[this] def hookAfterUpdating(
    implicit advertisedInfo: AdvertisedInfo,
    executionContext: ExecutionContext,
    serviceCollie: ServiceCollie
  ): HookAfterUpdating[Node] = updateRuntimeInfo(_)

  private[this] def checkConflict(nodeName: String, serviceName: String, clusterInfos: Seq[ClusterInfo]): Unit = {
    val conflicted = clusterInfos.filter(_.nodeNames.contains(nodeName))
    if (conflicted.nonEmpty)
      throw new IllegalArgumentException(s"node:$nodeName is used by $serviceName.")
  }

  private[this] def hookOfUpdating(
    implicit
    objectChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfUpdating[Updating, Node] =
    (key: ObjectKey, updating: Updating, previousOption: Option[Node]) =>
      previousOption match {
        case None =>
          // Node does not use definition so we check the field manually
          if (updating.user.isEmpty)
            throw DeserializationException(s"user field is required", fieldNames = List("user"))
          if (updating.password.isEmpty)
            throw DeserializationException(s"password field is required", fieldNames = List("password"))
          creationToNode(
            Creation(
              hostname = key.name(),
              port = updating.port.getOrElse(22),
              user = updating.user.get,
              password = updating.password.get,
              tags = updating.tags.getOrElse(Map.empty)
            )
          )
        case Some(previous) =>
          objectChecker.checkList
            .allZookeepers()
            .allBrokers()
            .allWorkers()
            .allStreams()
            .allShabondis()
            .check()
            .map(
              report =>
                (
                  report.runningZookeepers,
                  report.runningBrokers,
                  report.runningWorkers,
                  report.runningStreams,
                  report.runningShabondis
                )
            )
            .flatMap {
              case (
                  zookeeperClusterInfos,
                  brokerClusterInfos,
                  workerClusterInfos,
                  streamClusterInfos,
                  shabondiClusterInfos
                  ) =>
                checkConflict(key.name, "zookeeper cluster", zookeeperClusterInfos)
                checkConflict(key.name, "broker cluster", brokerClusterInfos)
                checkConflict(key.name, "worker cluster", workerClusterInfos)
                checkConflict(key.name, "stream cluster", streamClusterInfos)
                checkConflict(key.name, "shabondi cluster", shabondiClusterInfos)
                creationToNode(
                  Creation(
                    hostname = key.name(),
                    port = updating.port.getOrElse(previous.port),
                    user = updating.user.getOrElse(previous.user),
                    password = updating.password.getOrElse(previous.password),
                    tags = updating.tags.getOrElse(previous.tags)
                  )
                )
            }
      }

  private[this] def hookBeforeDelete(
    implicit objectChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookBeforeDelete =
    (key: ObjectKey) =>
      objectChecker.checkList
        .allZookeepers()
        .allBrokers()
        .allWorkers()
        .allStreams()
        .allShabondis()
        .node(key)
        .check()
        .map(
          report =>
            (
              report.nodes.head,
              report.zookeeperClusterInfos.keys.toSeq,
              report.brokerClusterInfos.keys.toSeq,
              report.workerClusterInfos.keys.toSeq,
              report.streamClusterInfos.keys.toSeq
            )
        )
        .map {
          case (node, zookeeperClusterInfos, brokerClusterInfos, workerClusterInfos, streamClusterInfos) =>
            checkConflict(node.hostname, "zookeeper cluster", zookeeperClusterInfos)
            checkConflict(node.hostname, "broker cluster", brokerClusterInfos)
            checkConflict(node.hostname, "worker cluster", workerClusterInfos)
            checkConflict(node.hostname, "stream cluster", streamClusterInfos)
        }
        .recover {
          // the duplicate deletes are legal to ohara
          case e: DataCheckException if e.nonexistent.contains(key) => ()
          case e: Throwable                                         => throw e
        }
        .map(_ => ())

  def apply(
    implicit store: DataStore,
    advertisedInfo: AdvertisedInfo,
    objectChecker: DataChecker,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): server.Route =
    RouteBuilder[Creation, Updating, Node]()
      .prefix(PREFIX)
      .hookOfCreation(hookOfCreation)
      .hookAfterCreation(hookAfterCreation)
      .hookOfUpdating(hookOfUpdating)
      .hookAfterUpdating(hookAfterUpdating)
      .hookOfGet(hookOfGet)
      .hookOfList(hookOfList)
      .hookBeforeDelete(hookBeforeDelete)
      .build()
}
