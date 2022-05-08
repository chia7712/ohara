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
import oharastream.ohara.agent._
import oharastream.ohara.client.configurator.BrokerApi
import oharastream.ohara.client.configurator.BrokerApi.{Creation, _}
import oharastream.ohara.client.configurator.ShabondiApi.ShabondiClusterInfo
import oharastream.ohara.client.configurator.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.TopicApi.TopicInfo
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook.{HookBeforeDelete, HookOfAction, HookOfCreation, HookOfUpdating}
import oharastream.ohara.configurator.store.{DataStore, MetricsCache}

import scala.concurrent.{ExecutionContext, Future}
object BrokerRoute {
  private[this] def creationToClusterInfo(
    creation: Creation
  )(implicit dataChecker: DataChecker, executionContext: ExecutionContext): Future[BrokerClusterInfo] =
    dataChecker.checkList
      .nodeNames(creation.nodeNames)
      .zookeeperCluster(creation.zookeeperClusterKey)
      .references(creation.raw, DEFINITIONS)
      .check()
      .map { _ =>
        BrokerClusterInfo(
          settings = creation.raw,
          aliveNodes = Set.empty,
          state = None,
          error = None,
          lastModified = CommonUtils.current()
        )
      }

  private[this] def hookOfCreation(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfCreation[Creation, BrokerClusterInfo] =
    creationToClusterInfo(_)

  private[this] def hookOfUpdating(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfUpdating[Updating, BrokerClusterInfo] =
    (key: ObjectKey, updating: Updating, previousOption: Option[BrokerClusterInfo]) =>
      previousOption match {
        case None =>
          creationToClusterInfo(
            BrokerApi.access.request
              .settings(updating.raw)
              // the key is not in update's settings so we have to add it to settings
              .key(key)
              .creation
          )
        case Some(previous) =>
          dataChecker.checkList.brokerCluster(key, DataCondition.STOPPED).check().flatMap { _ =>
            // 1) fill the previous settings (if exists)
            // 2) overwrite previous settings by updated settings
            // 3) fill the ignored settings by creation
            creationToClusterInfo(
              BrokerApi.access.request
                .settings(previous.settings)
                .settings(keepEditableFields(updating.raw, BrokerApi.DEFINITIONS))
                // the key is not in update's settings so we have to add it to settings
                .key(key)
                .creation
            )
          }
      }

  private[this] def hookOfStart(
    implicit dataChecker: DataChecker,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): HookOfAction[BrokerClusterInfo] =
    (brokerClusterInfo: BrokerClusterInfo, _, _) =>
      dataChecker.checkList
      // node names check is covered in super route
        .zookeeperCluster(brokerClusterInfo.zookeeperClusterKey, DataCondition.RUNNING)
        .allBrokers()
        .volumes(brokerClusterInfo.volumeMaps.keySet, DataCondition.RUNNING)
        .check()
        .map(_.runningBrokers)
        .flatMap { runningBrokerClusters =>
          val conflictBrokerClusters =
            runningBrokerClusters.filter(_.zookeeperClusterKey == brokerClusterInfo.zookeeperClusterKey)
          if (conflictBrokerClusters.nonEmpty)
            throw new IllegalArgumentException(
              s"zk cluster:${brokerClusterInfo.zookeeperClusterKey} is already used by broker cluster:${conflictBrokerClusters.head.name}"
            )
          serviceCollie.brokerCollie.creator
            .settings(brokerClusterInfo.settings)
            .name(brokerClusterInfo.name)
            .group(brokerClusterInfo.group)
            .clientPort(brokerClusterInfo.clientPort)
            .jmxPort(brokerClusterInfo.jmxPort)
            .zookeeperClusterKey(brokerClusterInfo.zookeeperClusterKey)
            .nodeNames(brokerClusterInfo.nodeNames)
            .threadPool(executionContext)
            .create()
        }
        .map(_ => ())

  private[this] def checkConflict(
    brokerClusterInfo: BrokerClusterInfo,
    workerClusterInfos: Seq[WorkerClusterInfo],
    streamClusterInfos: Seq[StreamClusterInfo],
    shabondiClusterInfos: Seq[ShabondiClusterInfo],
    topicInfos: Seq[TopicInfo]
  ): Unit = {
    val conflictWorkers = workerClusterInfos.filter(_.brokerClusterKey == brokerClusterInfo.key)
    if (conflictWorkers.nonEmpty)
      throw new IllegalArgumentException(
        s"you can't remove broker cluster:${brokerClusterInfo.key} since it is used by worker cluster:${conflictWorkers.map(_.key).mkString(",")}"
      )

    val conflictStreams = streamClusterInfos.filter(_.brokerClusterKey == brokerClusterInfo.key)
    if (conflictStreams.nonEmpty)
      throw new IllegalArgumentException(
        s"you can't remove broker cluster:${brokerClusterInfo.key} since it is used by stream cluster:${conflictStreams.map(_.key).mkString(",")}"
      )

    val conflictShabondis = shabondiClusterInfos.filter(_.brokerClusterKey == brokerClusterInfo.key)
    if (conflictShabondis.nonEmpty)
      throw new IllegalArgumentException(
        s"you can't remove broker cluster:${brokerClusterInfo.key} since it is used by shabondi cluster:${conflictShabondis.map(_.key).mkString(",")}"
      )

    val conflictTopics = topicInfos.filter(_.brokerClusterKey == brokerClusterInfo.key)
    if (conflictTopics.nonEmpty)
      throw new IllegalArgumentException(
        s"you can't remove broker cluster:${brokerClusterInfo.key} since it is used by topic:${conflictTopics.map(_.key).mkString(",")}"
      )
  }

  private[this] def hookBeforeStop(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfAction[BrokerClusterInfo] =
    (brokerClusterInfo: BrokerClusterInfo, _: String, _: Map[String, String]) =>
      dataChecker.checkList
        .allWorkers()
        .allStreams()
        .allShabondis()
        .allTopics()
        .check()
        .map(report => (report.runningWorkers, report.runningStreams, report.runningShabondis, report.runningTopics))
        .map {
          case (workerClusterInfos, streamClusterInfos, shabondiClusterInfos, topicInfos) =>
            checkConflict(brokerClusterInfo, workerClusterInfos, streamClusterInfos, shabondiClusterInfos, topicInfos)
        }

  private[this] def hookBeforeDelete(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookBeforeDelete =
    key =>
      dataChecker.checkList
        .allWorkers()
        .allStreams()
        .allShabondis()
        .allTopics()
        .brokerCluster(key, DataCondition.STOPPED)
        .check()
        .map(
          report =>
            (
              report.brokerClusterInfos.head._1,
              report.workerClusterInfos.keys.toSeq,
              report.streamClusterInfos.keys.toSeq,
              report.shabondiClusterInfos.keys.toSeq,
              report.topicInfos.keys.toSeq
            )
        )
        .map {
          case (brokerClusterInfo, workerClusterInfos, streamClusterInfos, shabondiClusterInfos, topicInfos) =>
            checkConflict(brokerClusterInfo, workerClusterInfos, streamClusterInfos, shabondiClusterInfos, topicInfos)
        }
        .recover {
          // the duplicate deletes are legal to ohara
          case e: DataCheckException if e.nonexistent.contains(key) => ()
          case e: Throwable                                         => throw e
        }
        .map(_ => ())

  def apply(
    implicit store: DataStore,
    dataChecker: DataChecker,
    meterCache: MetricsCache,
    brokerCollie: BrokerCollie,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): server.Route =
    clusterRoute[BrokerClusterInfo, Creation, Updating](
      root = PREFIX,
      hookOfCreation = hookOfCreation,
      hookOfUpdating = hookOfUpdating,
      hookOfStart = hookOfStart,
      hookBeforeStop = hookBeforeStop,
      hookBeforeDelete = hookBeforeDelete
    )
}
