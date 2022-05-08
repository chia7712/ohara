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
import oharastream.ohara.agent.ServiceCollie
import oharastream.ohara.client.configurator.ClusterInfo
import oharastream.ohara.client.configurator.VolumeApi._
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook._
import oharastream.ohara.configurator.store.DataStore
import spray.json.DeserializationException

import scala.concurrent.{ExecutionContext, Future}

private[configurator] object VolumeRoute {
  private[this] def hookOfCreation: HookOfCreation[Creation, Volume] =
    (creation: Creation) =>
      Future.successful(
        Volume(
          group = creation.group,
          name = creation.name,
          nodeNames = creation.nodeNames,
          path = creation.path,
          state = None,
          error = None,
          tags = creation.tags,
          lastModified = CommonUtils.current()
        )
      )

  private[this] def hookOfUpdating: HookOfUpdating[Updating, Volume] =
    (key: ObjectKey, updating: Updating, previousOption: Option[Volume]) =>
      hookOfCreation(previousOption match {
        case None =>
          if (updating.nodeNames.isEmpty)
            throw DeserializationException("nodeNames is required", fieldNames = List("nodeNames"))
          if (updating.path.isEmpty) throw DeserializationException("path is required", fieldNames = List("path"))
          Creation(
            group = key.group(),
            name = key.name(),
            nodeNames = updating.nodeNames.get,
            path = updating.path.get,
            tags = updating.tags.getOrElse(Map.empty)
          )
        case Some(previous) =>
          Creation(
            group = key.group(),
            name = key.name(),
            nodeNames = updating.nodeNames.getOrElse(previous.nodeNames),
            path = updating.path.getOrElse(previous.path),
            tags = updating.tags.getOrElse(previous.tags)
          )
      })

  private[this] def hookOfGet(
    implicit serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): HookOfGet[Volume] =
    (volume: Volume) =>
      serviceCollie.volumes().map(_.find(_.key == volume.key)).map {
        case None => volume
        case Some(runningVolume) =>
          volume.copy(state = runningVolume.state, error = runningVolume.error)
      }

  private[this] def hookOfList(
    implicit serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): HookOfList[Volume] =
    (volumes: Seq[Volume]) =>
      serviceCollie.volumes().map { runningVolumes =>
        volumes.map { volume =>
          runningVolumes.find(_.key == volume.key) match {
            case None => volume
            case Some(runningVolume) =>
              volume.copy(state = runningVolume.state, error = runningVolume.error)
          }
        }
      }

  private[this] def hookOfStart(
    implicit dataChecker: DataChecker,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): HookOfAction[Volume] =
    (volume: Volume, _, _) =>
      dataChecker.checkList
        .volume(volume.key)
        .check()
        .map(_.volumes.head)
        .flatMap {
          case (volume, condition) =>
            condition match {
              case DataCondition.RUNNING => Future.unit
              case DataCondition.STOPPED =>
                serviceCollie.createLocalVolumes(volume.key, volume.path, volume.nodeNames)
            }
        }

  /**
    * throw exception if the volume is used by service
    * @param volumeKey volume key
    */
  private[this] def check(
    volumeKey: ObjectKey,
    clusterInfo: ClusterInfo
  ): Unit =
    if (clusterInfo.volumeMaps.keys.exists(_ == volumeKey))
      throw new IllegalArgumentException(s"volume: $volumeKey is used by ${clusterInfo.kind}: ${clusterInfo.key}")

  private[this] def hookOfStop(
    implicit dataChecker: DataChecker,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): HookOfAction[Volume] =
    (volume: Volume, _, _) =>
      dataChecker.checkList
        .allZookeepers()
        .allBrokers()
        .allWorkers()
        .allStreams()
        .volume(volume.key)
        .check()
        .map(
          report =>
            (
              report.volumes.head._2,
              report.runningZookeepers,
              report.runningBrokers,
              report.runningWorkers,
              report.runningStreams
            )
        )
        .flatMap {
          case (condition, runningZookeepers, runningBrokers, runningWorkers, runningStreams) =>
            condition match {
              case DataCondition.STOPPED => Future.unit
              case DataCondition.RUNNING =>
                runningZookeepers.foreach(check(volume.key, _))
                runningBrokers.foreach(check(volume.key, _))
                runningWorkers.foreach(check(volume.key, _))
                runningStreams.foreach(check(volume.key, _))
                serviceCollie.removeVolumes(volume.key)
            }
        }

  private[this] def hookBeforeDelete(
    implicit dataChecker: DataChecker,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): HookBeforeDelete =
    volumeKey =>
      dataChecker.checkList
        .allZookeepers()
        .allBrokers()
        .allWorkers()
        .allStreams()
        .volume(volumeKey, DataCondition.STOPPED)
        .check()
        .map(
          report =>
            (
              report.zookeeperClusterInfos.keys,
              report.brokerClusterInfos.keys,
              report.workerClusterInfos.keys,
              report.streamClusterInfos.keys
            )
        )
        .flatMap {
          case (zookeepers, brokers, workers, streams) =>
            zookeepers.foreach(check(volumeKey, _))
            brokers.foreach(check(volumeKey, _))
            workers.foreach(check(volumeKey, _))
            streams.foreach(check(volumeKey, _))
            serviceCollie.removeVolumes(volumeKey)
        }

  private[route] def addNewNode(
    volume: Volume,
    nodeName: String
  )(implicit store: DataStore, serviceCollie: ServiceCollie, executionContext: ExecutionContext): Future[Unit] =
    serviceCollie
      .createLocalVolumes(volume.key, volume.path, volume.nodeNames)
      // update the new data
      .flatMap(_ => store.add(volume.newNodeNames(volume.nodeNames + nodeName)))
      .flatMap(_ => Future.unit)

  def apply(
    implicit store: DataStore,
    dataChecker: DataChecker,
    serviceCollie: ServiceCollie,
    executionContext: ExecutionContext
  ): server.Route =
    RouteBuilder[Creation, Updating, Volume]()
      .prefix(PREFIX)
      .hookOfCreation(hookOfCreation)
      .hookOfUpdating(hookOfUpdating)
      .hookOfGet(hookOfGet)
      .hookOfList(hookOfList)
      .hookBeforeDelete(hookBeforeDelete)
      .hookOfPutAction(START_COMMAND, hookOfStart)
      .hookOfPutAction(STOP_COMMAND, hookOfStop)
      .hookOfFinalPutAction((volume: Volume, nodeName: String, _: Map[String, String]) => addNewNode(volume, nodeName))
      .build()
}
