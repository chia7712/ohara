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
import oharastream.ohara.agent.{BrokerCollie, ServiceCollie, WorkerCollie}
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.FileInfoApi.FileInfo
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.client.configurator.ObjectApi.ObjectInfo
import oharastream.ohara.client.configurator.PipelineApi._
import oharastream.ohara.client.configurator.ShabondiApi.ShabondiClusterInfo
import oharastream.ohara.client.configurator.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.TopicApi.{TopicInfo, TopicState}
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.client.configurator.{
  BrokerApi,
  ConnectorApi,
  Data,
  FileInfoApi,
  NodeApi,
  ObjectApi,
  PipelineApi,
  ShabondiApi,
  StreamApi,
  TopicApi,
  WorkerApi,
  ZookeeperApi
}
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook._
import oharastream.ohara.configurator.store.{DataStore, MetricsCache}

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

private[configurator] object PipelineRoute {
  private[this] lazy val LOG = Logger(PipelineRoute.getClass)

  private[this] def toAbstract(obj: Data)(
    implicit dataStore: DataStore,
    serviceCollie: ServiceCollie,
    brokerCollie: BrokerCollie,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): Future[ObjectAbstract] = obj match {
    case data: ConnectorInfo =>
      connectorAdmin(data.workerClusterKey) { (clusterInfo, connectorAdmin) =>
        connectorAdmin
          .connectorDefinition(data.className)
          .map(
            classInfo =>
              ObjectAbstract(
                group = data.group,
                name = data.name,
                kind = classInfo.classType.key(),
                className = Some(data.className),
                state = None,
                error = None,
                // the group of counter is equal to connector's name (this is a part of kafka's core setting)
                // Hence, we filter the connectors having different "name" (we use name instead of name in creating connector)
                nodeMetrics = meterCache.meters(clusterInfo, data.key),
                lastModified = data.lastModified,
                tags = data.tags
              )
          )
          .flatMap { obj =>
            connectorAdmin
              .status(data.key)
              .map { connectorInfo =>
                obj.copy(
                  state = Some(connectorInfo.connector.state),
                  error = connectorInfo.connector.trace
                )
              }
              // we don't put this recovery in the final chain since we want to keep the definitions fetched from kafka
              .recover {
                case e: Throwable =>
                  LOG.debug(s"failed to fetch obj for $data", e)
                  obj
              }
          }
      }
    case data: TopicInfo =>
      topicAdmin(data.brokerClusterKey) { (clusterInfo, topicAdmin) =>
        topicAdmin
          .exist(data.key)
          .toScala
          .map { existent =>
            try if (existent) Some(TopicState.RUNNING) else None
            finally topicAdmin.close()
          }
          .map(_.map(_.name))
          .map(
            state =>
              ObjectAbstract(
                group = data.group,
                name = data.name,
                kind = data.kind,
                className = None,
                state = state,
                error = None,
                // noted we create a topic with name rather than name
                nodeMetrics = meterCache.meters(clusterInfo, data.key),
                lastModified = data.lastModified,
                tags = data.tags
              )
          )
      }
    case data @ (_: ZookeeperClusterInfo | _: BrokerClusterInfo | _: WorkerClusterInfo | _: StreamClusterInfo |
        _: ShabondiClusterInfo) =>
      serviceCollie
        .clusters()
        .map { clusters =>
          val status = clusters.find(_.kind.toString.toLowerCase == data.kind.toLowerCase)
          ObjectAbstract(
            group = data.group,
            name = data.name,
            kind = data.kind,
            className = data match {
              case clusterInfo: StreamClusterInfo   => Some(clusterInfo.className)
              case clusterInfo: ShabondiClusterInfo => Some(clusterInfo.shabondiClass)
              case _                                => None
            },
            state = status.flatMap(_.state).map(_.name),
            error = status.flatMap(_.error),
            nodeMetrics = data match {
              case clusterInfo: StreamClusterInfo   => meterCache.meters(clusterInfo, clusterInfo.key)
              case clusterInfo: ShabondiClusterInfo => meterCache.meters(clusterInfo, clusterInfo.key)
              case _                                => Map.empty
            },
            lastModified = data.lastModified,
            tags = data.tags
          )
        }
    case _ =>
      Future.successful(
        ObjectAbstract(
          group = obj.group,
          name = obj.name,
          kind = obj.kind,
          className = None,
          state = None,
          error = None,
          nodeMetrics = Map.empty,
          lastModified = obj.lastModified,
          tags = obj.tags
        )
      )
  }

  /**
    * collect the abstract for all objects in endpoints. This is a expensive operation since it invokes a bunch of threads
    * to retrieve the information from many remote nodes.
    * @param pipeline pipeline
    * @param store data store
    * @param executionContext thread pool
    * @param meterCache meter cache
    * @return updated pipeline
    */
  private[this] def updateObjectsAndJarKeys(pipeline: Pipeline)(
    implicit brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): Future[Pipeline] =
    Future
      .traverse(pipeline.endpoints) { d =>
        d.kind match {
          case ConnectorApi.KIND =>
            store.get[ConnectorInfo](d.key)
          case TopicApi.KIND =>
            store.get[TopicInfo](d.key)
          case ZookeeperApi.KIND =>
            store.get[ZookeeperClusterInfo](d.key)
          case BrokerApi.KIND =>
            store.get[BrokerClusterInfo](d.key)
          case WorkerApi.KIND =>
            store.get[WorkerClusterInfo](d.key)
          case StreamApi.KIND =>
            store.get[StreamClusterInfo](d.key)
          case ShabondiApi.KIND =>
            store.get[ShabondiClusterInfo](d.key)
          case FileInfoApi.KIND =>
            store.get[FileInfo](d.key)
          case PipelineApi.KIND =>
            store.get[Pipeline](d.key)
          case ObjectApi.KIND =>
            store.get[ObjectInfo](d.key)
          case NodeApi.KIND =>
            store.get[Node](d.key)
          case _ =>
            // TODO: this is a workaround if we forget to add the object match in adding new APIs ...
            store.raws(d.key).map(_.headOption)
        }
      }
      .map(_.flatten.toSet)
      .flatMap(Future.traverse(_) { obj =>
        toAbstract(obj).recover {
          case e: Throwable =>
            ObjectAbstract(
              group = obj.group,
              name = obj.name,
              kind = obj.kind,
              className = obj match {
                case d: ConnectorInfo       => Some(d.className)
                case d: StreamClusterInfo   => Some(d.className)
                case d: ShabondiClusterInfo => Some(d.shabondiClass)
                case _                      => None
              },
              state = None,
              error = Some(e.getMessage),
              nodeMetrics = Map.empty,
              lastModified = obj.lastModified,
              tags = obj.tags
            )
        }
      })
      .map(objects => pipeline.copy(objects = objects))
      // update the jar keys used by the objects
      .flatMap { pipeline =>
        Future
          .traverse(pipeline.objects.map(obj => ObjectKey.of(obj.group, obj.name)))(store.raws)
          .map(_.flatten)
          .map(_.map {
            case connectorInfo: ConnectorInfo =>
              store
                .get[WorkerClusterInfo](connectorInfo.workerClusterKey)
                .map(_.map(w => w.sharedJarKeys ++ w.pluginKeys).getOrElse(Set.empty))
            case streamClusterInfo: StreamClusterInfo => Future.successful(Set(streamClusterInfo.jarKey))
            case _                                    => Future.successful(Set.empty[ObjectKey])
          })
          .flatMap(s => Future.sequence(s))
          .map(_.flatten)
          .map(jarKeys => pipeline.copy(jarKeys = jarKeys))
      }

  private[this] def hookOfGet(
    implicit brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): HookOfGet[Pipeline] = updateObjectsAndJarKeys(_)

  private[this] def hookOfList(
    implicit brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): HookOfList[Pipeline] =
    Future.traverse(_)(updateObjectsAndJarKeys)

  private[this] def hookOfCreation(
    implicit brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): HookOfCreation[Creation, Pipeline] =
    (creation: Creation) =>
      updateObjectsAndJarKeys(
        Pipeline(
          group = creation.group,
          name = creation.name,
          endpoints = creation.endpoints,
          objects = Set.empty,
          jarKeys = Set.empty,
          lastModified = CommonUtils.current(),
          tags = creation.tags
        )
      )

  private[this] def hookOfUpdating(
    implicit brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): HookOfUpdating[Updating, Pipeline] =
    (key: ObjectKey, update: Updating, previous: Option[Pipeline]) =>
      updateObjectsAndJarKeys(
        Pipeline(
          group = key.group,
          name = key.name,
          endpoints = update.endpoints.getOrElse(previous.map(_.endpoints).getOrElse(Set.empty)),
          objects = previous.map(_.objects).getOrElse(Set.empty),
          jarKeys = previous.map(_.jarKeys).getOrElse(Set.empty),
          lastModified = CommonUtils.current(),
          tags = update.tags.getOrElse(previous.map(_.tags).getOrElse(Map.empty))
        )
      )

  private[this] def hookBeforeDelete: HookBeforeDelete = _ => Future.unit

  private[this] def refreshEndpoints(
    pipeline: Pipeline
  )(implicit store: DataStore, executionContext: ExecutionContext): Future[Pipeline] =
    Future
      .traverse(pipeline.endpoints)(
        endpoint =>
          (endpoint.kind match {
            case TopicApi.KIND     => store.get[TopicInfo](endpoint.key)
            case ConnectorApi.KIND => store.get[ConnectorInfo](endpoint.key)
            case FileInfoApi.KIND  => store.get[FileInfo](endpoint.key)
            case NodeApi.KIND      => store.get[Node](endpoint.key)
            case ObjectApi.KIND    => store.get[ObjectInfo](endpoint.key)
            case PipelineApi.KIND  => store.get[Pipeline](endpoint.key)
            case ZookeeperApi.KIND => store.get[ZookeeperClusterInfo](endpoint.key)
            case BrokerApi.KIND    => store.get[BrokerClusterInfo](endpoint.key)
            case WorkerApi.KIND    => store.get[WorkerClusterInfo](endpoint.key)
            case StreamApi.KIND    => store.get[StreamClusterInfo](endpoint.key)
            case ShabondiApi.KIND  => store.get[ShabondiClusterInfo](endpoint.key)
            case _                 => Future.successful(None)
          }).map {
            case None    => Seq.empty
            case Some(o) => Seq(o)
          }
      )
      .map(_.flatten)
      .map(
        existedData =>
          pipeline.copy(
            endpoints = pipeline.endpoints
              .filter(
                endpoint =>
                  existedData.exists(
                    data =>
                      data.group == endpoint.group
                        && data.name == endpoint.name
                        && endpoint.kind.contains(data.kind)
                  )
              )
          )
      )

  private[this] def hookOfRefresh(
    implicit store: DataStore,
    executionContext: ExecutionContext
  ): HookOfAction[Pipeline] =
    (pipeline: Pipeline, _, _) =>
      refreshEndpoints(pipeline)
        .flatMap(pipeline => store.add[Pipeline](pipeline))
        .map(_ => ())

  def apply(
    implicit brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    store: DataStore,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): server.Route =
    RouteBuilder[Creation, Updating, Pipeline]()
      .prefix(PREFIX)
      .hookOfCreation(hookOfCreation)
      .hookOfUpdating(hookOfUpdating)
      .hookOfGet(hookOfGet)
      .hookOfList(hookOfList)
      .hookBeforeDelete(hookBeforeDelete)
      .hookOfPutAction(REFRESH_COMMAND, hookOfRefresh)
      .build()
}
