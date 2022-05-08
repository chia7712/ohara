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
import oharastream.ohara.agent.{ServiceCollie, ZookeeperCollie}
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ZookeeperApi
import oharastream.ohara.client.configurator.ZookeeperApi._
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook.{HookBeforeDelete, HookOfAction, HookOfCreation, HookOfUpdating}
import oharastream.ohara.configurator.store.{DataStore, MetricsCache}

import scala.concurrent.{ExecutionContext, Future}

object ZookeeperRoute {
  private[this] def creationToClusterInfo(
    creation: Creation
  )(implicit dataChecker: DataChecker, executionContext: ExecutionContext): Future[ZookeeperClusterInfo] =
    dataChecker.checkList
      .nodeNames(creation.nodeNames)
      .references(creation.raw, DEFINITIONS)
      .check()
      .map { _ =>
        ZookeeperClusterInfo(
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
  ): HookOfCreation[Creation, ZookeeperClusterInfo] =
    creationToClusterInfo(_)

  private[this] def hookOfUpdating(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfUpdating[Updating, ZookeeperClusterInfo] =
    (key: ObjectKey, updating: Updating, previousOption: Option[ZookeeperClusterInfo]) =>
      previousOption match {
        case None => creationToClusterInfo(access.request.settings(updating.raw).key(key).creation)
        case Some(previous) =>
          dataChecker.checkList.zookeeperCluster(key, DataCondition.STOPPED).check().flatMap { _ =>
            creationToClusterInfo(
              // 1) fill the previous settings (if exists)
              // 2) overwrite previous settings by updated settings
              // 3) fill the ignored settings by creation
              access.request
                .settings(previous.settings)
                .settings(keepEditableFields(updating.raw, ZookeeperApi.DEFINITIONS))
                // the key is not in update's settings so we have to add it to settings
                .key(key)
                .creation
            )
          }
      }

  private[this] def hookOfStart(
    implicit serviceCollie: ServiceCollie,
    dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfAction[ZookeeperClusterInfo] =
    (zookeeperClusterInfo: ZookeeperClusterInfo, _, _) =>
      dataChecker.checkList
      // node names check is covered in super route
        .volumes(zookeeperClusterInfo.volumeMaps.keySet, DataCondition.RUNNING)
        .check()
        .flatMap { _ =>
          serviceCollie.zookeeperCollie.creator
            .settings(zookeeperClusterInfo.settings)
            .name(zookeeperClusterInfo.name)
            .group(zookeeperClusterInfo.group)
            .clientPort(zookeeperClusterInfo.clientPort)
            .electionPort(zookeeperClusterInfo.electionPort)
            .peerPort(zookeeperClusterInfo.peerPort)
            .nodeNames(zookeeperClusterInfo.nodeNames)
            .threadPool(executionContext)
            .create()
        }

  private[this] def checkConflict(
    zookeeperClusterInfo: ZookeeperClusterInfo,
    brokerClusterInfos: Seq[BrokerClusterInfo]
  ): Unit = {
    val conflictedBrokers = brokerClusterInfos.filter(_.zookeeperClusterKey == zookeeperClusterInfo.key)
    if (conflictedBrokers.nonEmpty)
      throw new IllegalArgumentException(
        s"you can't remove zookeeper cluster:${zookeeperClusterInfo.key} since it is used by broker cluster:${conflictedBrokers.map(_.key).mkString(",")}"
      )
  }

  private[this] def hookBeforeStop(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfAction[ZookeeperClusterInfo] =
    (zookeeperClusterInfo: ZookeeperClusterInfo, _: String, _: Map[String, String]) =>
      dataChecker.checkList.allBrokers().check().map(_.runningBrokers).map { runningBrokerClusters =>
        checkConflict(zookeeperClusterInfo, runningBrokerClusters)
      }

  private[this] def hookBeforeDelete(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookBeforeDelete =
    key =>
      dataChecker.checkList
        .zookeeperCluster(key, DataCondition.STOPPED)
        .allBrokers()
        .check()
        .map(report => (report.zookeeperClusterInfos.head._1, report.brokerClusterInfos.keys.toSeq))
        .map {
          case (zookeeperClusterInfo, brokerClusterInfos) =>
            checkConflict(zookeeperClusterInfo, brokerClusterInfos)
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
    zookeeperCollie: ZookeeperCollie,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): server.Route =
    clusterRoute[ZookeeperClusterInfo, Creation, Updating](
      root = PREFIX,
      hookOfCreation = hookOfCreation,
      hookOfUpdating = hookOfUpdating,
      hookOfStart = hookOfStart,
      hookBeforeStop = hookBeforeStop,
      hookBeforeDelete = hookBeforeDelete
    )
}
