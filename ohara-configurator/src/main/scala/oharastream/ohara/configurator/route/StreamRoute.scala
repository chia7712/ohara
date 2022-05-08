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
import oharastream.ohara.client.configurator.StreamApi
import oharastream.ohara.client.configurator.StreamApi._
import oharastream.ohara.common.setting.{ClassType, ObjectKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook.{HookBeforeDelete, HookOfAction, HookOfCreation, HookOfUpdating}
import oharastream.ohara.configurator.store.{DataStore, MetricsCache}
import oharastream.ohara.stream.config.StreamDefUtils
import spray.json.DeserializationException

import scala.concurrent.{ExecutionContext, Future}
private[configurator] object StreamRoute {
  private[this] def creationToClusterInfo(creation: Creation)(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): Future[StreamClusterInfo] =
    dataChecker.checkList
      .nodeNames(creation.nodeNames)
      .file(creation.jarKey)
      .brokerCluster(creation.brokerClusterKey)
      /**
        * TODO: this is a workaround to avoid input multiple topics
        * TODO: please refactor this after the single from/to topic issue resolved...by Sam
        */
      .topics {
        if (creation.fromTopicKeys.size > 1)
          throw new IllegalArgumentException(
            s"the size of from topics MUST be equal to 1 (multiple topics is a unsupported feature)"
          )
        creation.fromTopicKeys
      }
      /**
        * TODO: this is a workaround to avoid input multiple topics
        * TODO: please refactor this after the single from/to topic issue resolved...by Sam
        */
      .topics {
        if (creation.toTopicKeys.size > 1)
          throw new IllegalArgumentException(
            s"the size of from topics MUST be equal to 1 (multiple topics is a unsupported feature)"
          )
        creation.toTopicKeys
      }
      .references(creation.raw, DEFINITIONS)
      .check()
      .map(_.fileInfos)
      .map { fileInfos =>
        val available = fileInfos.flatMap(_.classInfos).filter(_.classType == ClassType.STREAM).map(_.className)
        val className = creation.className.getOrElse {
          available.size match {
            case 0 =>
              throw DeserializationException(
                s"no available stream classes from files:${fileInfos.map(_.key).mkString(",")}"
              )
            case 1 => available.head
            case _ =>
              throw DeserializationException(
                s"too many alternatives:${available.mkString(",")} from files:${fileInfos.map(_.key).mkString(",")}"
              )
          }
        }

        if (!available.contains(className))
          throw DeserializationException(s"the class:$className is not in files:${fileInfos.map(_.key).mkString(",")}")

        StreamClusterInfo(
          settings = CREATION_FORMAT.toBuilder
            .definitions(
              fileInfos
                .flatMap(_.classInfos)
                .filter(_.className == className)
                .head
                .settingDefinitions
                // we should add definition having default value to complete Creation request but
                // TODO: we should check all definitions in Creation phase
                // https://github.com/oharastream/ohara/issues/4506
                .filter(_.hasDefault)
            )
            .build()
            .refine(StreamApi.access.request.settings(creation.raw).className(className).creation)
            .raw,
          aliveNodes = Set.empty,
          state = None,
          nodeMetrics = Map.empty,
          error = None,
          lastModified = CommonUtils.current()
        )
      }

  private[this] def hookOfCreation(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfCreation[Creation, StreamClusterInfo] =
    creationToClusterInfo(_)

  private[this] def hookOfUpdating(
    implicit dataChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookOfUpdating[Updating, StreamClusterInfo] =
    (key: ObjectKey, updating: Updating, previousOption: Option[StreamClusterInfo]) =>
      previousOption match {
        case None =>
          creationToClusterInfo(
            access.request
              .settings(updating.raw)
              // the key is not in update's settings so we have to add it to settings
              .key(key)
              .creation
          )
        case Some(previous) =>
          dataChecker.checkList
          // we don't support to update a running stream
            .stream(previous.key, DataCondition.STOPPED)
            .check()
            .flatMap { _ =>
              // 1) fill the previous settings (if exists)
              // 2) overwrite previous settings by updated settings
              // 3) fill the ignored settings by creation
              creationToClusterInfo(
                access.request
                  .settings(previous.settings)
                  .settings(keepEditableFields(updating.raw, StreamApi.DEFINITIONS))
                  // the key is not in update's settings so we have to add it to settings
                  .key(key)
                  .creation
              )
            }
      }

  private[this] def hookOfStart(
    implicit dataChecker: DataChecker,
    streamCollie: StreamCollie,
    executionContext: ExecutionContext
  ): HookOfAction[StreamClusterInfo] =
    (streamClusterInfo: StreamClusterInfo, _, _) => {
      // This "optional" is for our UI since it does not require users to define the node names
      // when creating pipeline. Noted that our services (zk, bk and wk) still require user to
      // define the node names in creating.
      if (streamClusterInfo.nodeNames.isEmpty)
        throw DeserializationException(s"the node names can't be empty", fieldNames = List("nodeNames"))
      dataChecker.checkList
      // node names check is covered in super route
        .stream(streamClusterInfo.key)
        .file(streamClusterInfo.jarKey)
        .brokerCluster(streamClusterInfo.brokerClusterKey, DataCondition.RUNNING)
        .volumes(streamClusterInfo.volumeMaps.keySet, DataCondition.RUNNING)
        .topics(
          // our UI needs to create a stream without topics so the stream info may has no topics...
          if (streamClusterInfo.toTopicKeys.isEmpty)
            throw DeserializationException(
              s"${StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key()} can't be empty",
              fieldNames = List(StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key())
            )
          else streamClusterInfo.toTopicKeys,
          DataCondition.RUNNING
        )
        .topics(
          // our UI needs to create a stream without topics so the stream info may has no topics...
          if (streamClusterInfo.fromTopicKeys.isEmpty)
            throw DeserializationException(
              s"${StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key()} topics can't be empty",
              fieldNames = List(StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key())
            )
          else streamClusterInfo.fromTopicKeys,
          DataCondition.RUNNING
        )
        .check()
        .map(
          report =>
            (
              report.streamClusterInfos.head._2,
              report.fileInfos.head,
              report.brokerClusterInfos.head._1,
              report.runningTopics
            )
        )
        .flatMap {
          case (condition, fileInfo, brokerClusterInfo, topicInfos) =>
            condition match {
              case DataCondition.RUNNING => Future.unit
              case DataCondition.STOPPED =>
                topicInfos.filter(_.brokerClusterKey != brokerClusterInfo.key).foreach { topicInfo =>
                  throw new IllegalArgumentException(
                    s"stream app counts on broker cluster:${streamClusterInfo.brokerClusterKey} " +
                      s"but topic:${topicInfo.key} is on another broker cluster:${topicInfo.brokerClusterKey}"
                  )
                }
                streamCollie.creator
                // these settings will send to container environment
                // we convert all value to string for convenient
                  .settings(streamClusterInfo.settings)
                  .name(streamClusterInfo.name)
                  .group(streamClusterInfo.group)
                  .nodeNames(streamClusterInfo.nodeNames)
                  .jarKey(fileInfo.key)
                  .brokerClusterKey(brokerClusterInfo.key)
                  .connectionProps(brokerClusterInfo.connectionProps)
                  .threadPool(executionContext)
                  .create()
            }
        }
    }

  private[this] def hookBeforeStop: HookOfAction[StreamClusterInfo] = (_, _, _) => Future.unit

  private[this] def hookBeforeDelete: HookBeforeDelete = _ => Future.unit

  def apply(
    implicit store: DataStore,
    dataChecker: DataChecker,
    streamCollie: StreamCollie,
    serviceCollie: ServiceCollie,
    meterCache: MetricsCache,
    executionContext: ExecutionContext
  ): server.Route =
    clusterRoute[StreamClusterInfo, Creation, Updating](
      root = PREFIX,
      hookOfCreation = hookOfCreation,
      hookOfUpdating = hookOfUpdating,
      hookOfStart = hookOfStart,
      hookBeforeStop = hookBeforeStop,
      hookBeforeDelete = hookBeforeDelete
    )
}
