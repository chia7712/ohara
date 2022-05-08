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
import oharastream.ohara.agent.WorkerCollie
import oharastream.ohara.client.configurator.ConnectorApi
import oharastream.ohara.client.configurator.ConnectorApi._
import oharastream.ohara.client.configurator.FileInfoApi.ClassInfo
import oharastream.ohara.common.setting.{ConnectorKey, ObjectKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.route.hook._
import oharastream.ohara.configurator.store.{DataStore, MetricsCache}
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils
import spray.json.DeserializationException

import scala.concurrent.{ExecutionContext, Future}
private[configurator] object ConnectorRoute {
  private[this] lazy val LOG = Logger(ConnectorRoute.getClass)

  private[this] def creationToConnectorInfo(
    creation: Creation
  )(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): Future[ConnectorInfo] =
    objectChecker.checkList
      .workerCluster(creation.workerClusterKey)
      .topics(creation.topicKeys)
      /**
        * TODO: we should add custom definitions to checks
        * https://github.com/oharastream/ohara/issues/4506
        */
      .references(creation.raw, DEFINITIONS)
      .check()
      .map(_.workerClusterInfos.head)
      // if the worker cluster is running, we try to fetch definitions and add them to the settings for the ignored key-values.
      .flatMap {
        case (workerClusterInfo, condition) =>
          condition match {
            case DataCondition.RUNNING =>
              try workerCollie.connectorAdmin(workerClusterInfo).flatMap(_.connectorDefinitions())
              catch {
                case e: Throwable =>
                  LOG.error(s"failed to get definitions from worker cluster:${workerClusterInfo.key}", e)
                  Future.successful(Map.empty[String, ClassInfo])
              }
            case DataCondition.STOPPED => Future.successful(Map.empty[String, ClassInfo])
          }
      }
      .map(_.get(creation.className).map(_.settingDefinitions).getOrElse(Seq.empty))
      .map { definitions =>
        ConnectorInfo(
          settings = CREATION_FORMAT.toBuilder
            .definitions(
              definitions
              // we should add definition having default value to complete Creation request but
              // TODO: we should check all definitions in Creation phase
              // https://github.com/oharastream/ohara/issues/4506
                .filter(_.hasDefault)
            )
            .build()
            .refine(creation)
            .raw,
          // we don't need to fetch connector from kafka since it has not existed in kafka.
          state = None,
          aliveNodes = Set.empty,
          error = None,
          tasksStatus = Seq.empty,
          nodeMetrics = Map.empty,
          lastModified = CommonUtils.current()
        )
      }

  private[this] def updateState(connectorInfo: ConnectorInfo)(
    implicit executionContext: ExecutionContext,
    workerCollie: WorkerCollie,
    objectChecker: DataChecker,
    meterCache: MetricsCache
  ): Future[ConnectorInfo] =
    objectChecker.checkList
      .workerCluster(connectorInfo.workerClusterKey)
      .check()
      .map(_.workerClusterInfos.head)
      .flatMap {
        case (workerClusterInfo, condition) =>
          condition match {
            case DataCondition.STOPPED =>
              Future.successful(
                connectorInfo.copy(
                  state = None,
                  error = None,
                  aliveNodes = Set.empty,
                  tasksStatus = Seq.empty,
                  nodeMetrics = Map.empty
                )
              )
            case DataCondition.RUNNING =>
              workerCollie.connectorAdmin(workerClusterInfo).flatMap { connectorAdmin =>
                // we check the active connectors first to avoid exception :)
                connectorAdmin.exist(connectorInfo.key).flatMap {
                  if (_) connectorAdmin.status(connectorInfo.key).map { connectorInfoFromKafka =>
                    val allStatus = connectorInfoFromKafka.tasks.map { taskStatus =>
                      Status(
                        state = State.forName(taskStatus.state),
                        error = taskStatus.trace,
                        nodeName = taskStatus.workerHostname,
                        coordinator = false
                      )
                    } :+ Status(
                      state = State.forName(connectorInfoFromKafka.connector.state),
                      error = connectorInfoFromKafka.connector.trace,
                      nodeName = connectorInfoFromKafka.connector.workerHostname,
                      coordinator = true
                    )

                    connectorInfo.copy(
                      // this is the "summary" of this connector
                      state =
                        // this connector is running only if there is a running coordinator and a running follower as least.
                        if (allStatus.count(_.state == State.RUNNING) >= 2) Some(State.RUNNING)
                        // this connector is in pending
                        else if (allStatus.isEmpty) None
                        else
                          allStatus
                            .filterNot(_.state == State.RUNNING)
                            .map(_.state)
                            .headOption
                            .orElse(Some(State.FAILED)),
                      error = allStatus.flatMap(_.error).headOption,
                      aliveNodes = allStatus.filter(_.state == State.RUNNING).map(_.nodeName).toSet,
                      tasksStatus = allStatus,
                      nodeMetrics = meterCache.meters(workerClusterInfo, connectorInfo.key)
                    )
                  } else
                    Future.successful(
                      connectorInfo.copy(
                        state = None,
                        error = None,
                        aliveNodes = Set.empty,
                        tasksStatus = Seq.empty,
                        nodeMetrics = Map.empty
                      )
                    )
                }
              }
          }
      }
      .recover {
        case e: Throwable =>
          LOG.debug(s"failed to fetch stats for $connectorInfo", e)
          connectorInfo.copy(
            state = None,
            error = Some(e.getMessage),
            aliveNodes = Set.empty,
            tasksStatus = Seq.empty,
            nodeMetrics = Map.empty
          )
      }

  private[this] def hookOfGet(
    implicit workerCollie: WorkerCollie,
    objectChecker: DataChecker,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): HookOfGet[ConnectorInfo] = updateState

  private[this] def hookOfList(
    implicit workerCollie: WorkerCollie,
    objectChecker: DataChecker,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): HookOfList[ConnectorInfo] =
    (connectorDescriptions: Seq[ConnectorInfo]) => Future.sequence(connectorDescriptions.map(updateState))

  private[this] def hookOfCreation(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): HookOfCreation[Creation, ConnectorInfo] =
    creationToConnectorInfo(_)

  private[this] def hookOfUpdating(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): HookOfUpdating[Updating, ConnectorInfo] =
    (key: ObjectKey, updating: Updating, previousOption: Option[ConnectorInfo]) =>
      previousOption match {
        case None =>
          creationToConnectorInfo(
            access.request
              .settings(updating.raw)
              // the key is not in update's settings so we have to add it to settings
              .name(key.name)
              .group(key.group)
              .creation
          )
        case Some(previous) =>
          objectChecker.checkList.connector(previous.key, DataCondition.STOPPED).check().flatMap { _ =>
            // 1) fill the previous settings (if exists)
            // 2) overwrite previous settings by updated settings
            // 3) fill the ignored settings by creation
            creationToConnectorInfo(
              access.request
                .settings(previous.settings)
                .settings(keepEditableFields(updating.raw, ConnectorApi.DEFINITIONS))
                // the key is not in update's settings so we have to add it to settings
                .name(key.name)
                .group(key.group)
                .creation
            )
          }
      }

  private[this] def hookBeforeDelete(
    implicit objectChecker: DataChecker,
    executionContext: ExecutionContext
  ): HookBeforeDelete =
    (key: ObjectKey) =>
      objectChecker.checkList
        .connector(ConnectorKey.of(key.group(), key.name()), DataCondition.STOPPED)
        .check()
        .recover {
          // the duplicate deletes are legal to ohara
          case e: DataCheckException if e.nonexistent.contains(key) => ()
        }
        .map(_ => ())

  private[this] def hookOfStart(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): HookOfAction[ConnectorInfo] =
    (connectorInfo: ConnectorInfo, _, _) =>
      objectChecker.checkList
        .connector(connectorInfo.key)
        .topics(
          // our UI needs to create a connector without topics so the connector info may has no topics...
          if (connectorInfo.topicKeys.isEmpty)
            throw DeserializationException(
              s"${ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key()} can't be empty",
              fieldNames = List(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key())
            )
          else connectorInfo.topicKeys,
          DataCondition.RUNNING
        )
        .workerCluster(connectorInfo.workerClusterKey, DataCondition.RUNNING)
        .check()
        .map(report => (report.connectorInfos.head._2, report.runningWorkers.head, report.runningTopics))
        .flatMap {
          case (condition, workerClusterInfo, topicInfos) =>
            condition match {
              case DataCondition.RUNNING => Future.unit
              case DataCondition.STOPPED =>
                topicInfos.filter(_.brokerClusterKey != workerClusterInfo.brokerClusterKey).foreach { topicInfo =>
                  throw new IllegalArgumentException(
                    s"Connector app counts on broker cluster:${workerClusterInfo.brokerClusterKey} " +
                      s"but topic:${topicInfo.key} is on another broker cluster:${topicInfo.brokerClusterKey}"
                  )
                }
                workerCollie.connectorAdmin(workerClusterInfo).flatMap {
                  _.connectorCreator()
                    .settings(connectorInfo.plain)
                    // always override the name
                    .connectorKey(connectorInfo.key)
                    .threadPool(executionContext)
                    .topicKeys(connectorInfo.topicKeys)
                    .create()
                    .map(_ => ())
                }
            }
        }

  private[this] def hookOfStop(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): HookOfAction[ConnectorInfo] =
    (connectorInfo: ConnectorInfo, _, _) =>
      objectChecker.checkList.connector(connectorInfo.key).check().map(_.connectorInfos.head._2).flatMap {
        case DataCondition.STOPPED => Future.unit
        case DataCondition.RUNNING =>
          objectChecker.checkList
            .workerCluster(connectorInfo.workerClusterKey, DataCondition.RUNNING)
            .check()
            .map(_.runningWorkers.head)
            .flatMap(workerCollie.connectorAdmin)
            .flatMap(connectorAdmin => connectorAdmin.delete(connectorInfo.key))
      }

  private[this] def hookOfPause(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): HookOfAction[ConnectorInfo] =
    (connectorInfo: ConnectorInfo, _, _) =>
      objectChecker.checkList
        .workerCluster(connectorInfo.workerClusterKey, DataCondition.RUNNING)
        .connector(connectorInfo.key, DataCondition.RUNNING)
        .check()
        .map(_.runningWorkers.head)
        .flatMap(workerCollie.connectorAdmin)
        .map { wkClient =>
          wkClient.status(connectorInfo.key).map(_.connector.state).flatMap {
            case State.PAUSED.name => Future.unit
            case _                 => wkClient.pause(connectorInfo.key).map(_ => ())
          }
        }

  private[this] def hookOfResume(
    implicit objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext
  ): HookOfAction[ConnectorInfo] =
    (connectorInfo: ConnectorInfo, _, _) =>
      objectChecker.checkList
        .workerCluster(connectorInfo.workerClusterKey, DataCondition.RUNNING)
        .connector(connectorInfo.key, DataCondition.RUNNING)
        .check()
        .map(_.runningWorkers.head)
        .flatMap(workerCollie.connectorAdmin)
        .map { wkClient =>
          wkClient.status(connectorInfo.key).map(_.connector.state).flatMap {
            case State.RUNNING.name => Future.unit
            case _                  => wkClient.resume(connectorInfo.key).map(_ => ())
          }
        }

  def apply(
    implicit store: DataStore,
    objectChecker: DataChecker,
    workerCollie: WorkerCollie,
    executionContext: ExecutionContext,
    meterCache: MetricsCache
  ): server.Route =
    RouteBuilder[Creation, Updating, ConnectorInfo]()
      .prefix(PREFIX)
      .hookOfCreation(hookOfCreation)
      .hookOfUpdating(hookOfUpdating)
      .hookOfGet(hookOfGet)
      .hookOfList(hookOfList)
      .hookBeforeDelete(hookBeforeDelete)
      .hookOfPutAction(START_COMMAND, hookOfStart)
      .hookOfPutAction(STOP_COMMAND, hookOfStop)
      .hookOfPutAction(PAUSE_COMMAND, hookOfPause)
      .hookOfPutAction(RESUME_COMMAND, hookOfResume)
      .build()
}
