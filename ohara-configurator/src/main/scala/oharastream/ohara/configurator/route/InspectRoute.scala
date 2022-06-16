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

import java.lang
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.{entity, _}
import oharastream.ohara.agent.{BrokerCollie, ServiceCollie, WorkerCollie}
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.FileInfoApi.{ClassInfo, FileInfo}
import oharastream.ohara.client.configurator.InspectApi._
import oharastream.ohara.client.configurator.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.client.configurator.{BrokerApi, ConnectorApi, ErrorApi, FileInfoApi, InspectApi, ShabondiApi, StreamApi, TopicApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.{ClassType, ConnectorKey, ObjectKey, TopicKey}
import oharastream.ohara.common.util.{ByteUtils, CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.configurator.Configurator.Mode
import oharastream.ohara.configurator.fake.{FakeConnectorAdmin, FakeServiceCollie}
import oharastream.ohara.configurator.store.DataStore
import oharastream.ohara.kafka.Consumer.Record
import oharastream.ohara.kafka.{Consumer, Header, TopicAdmin}
import oharastream.ohara.shabondi.ShabondiDefinitions
import oharastream.ohara.shabondi.common.JsonSupport
import oharastream.ohara.stream.config.StreamDefUtils
import spray.json.{DeserializationException, JsArray, JsNull, JsNumber, JsObject, JsString, JsTrue}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[configurator] object InspectRoute {
  private[this] def topicData(records: Seq[Record[Array[Byte], Array[Byte]]]): TopicData =
    TopicData(
      records.reverse
        .filter(_.key().isPresent)
        .map(record => (record.partition(), record.offset(), record.key().get(), record.headers().asScala))
        .map {
          case (partition, offset, bytes, headers) =>
            var error: Option[String] = None
            def swallowException[T](f: => Option[T]): Option[T] =
              try f
              catch {
                case e: Throwable =>
                  error = Some(e.getMessage)
                  None
              }

            Message(
              partition = partition,
              offset = offset,
              // only Ohara source connectors have this header
              sourceClass = swallowException(
                headers
                  .find(_.key() == Header.SOURCE_CLASS_KEY)
                  .map(_.value())
                  .map(ByteUtils.toString)
              ),
              sourceKey = swallowException(
                headers
                  .find(_.key() == Header.SOURCE_KEY_KEY)
                  .map(_.value())
                  .map(ByteUtils.toString)
                  .map(ObjectKey.toObjectKey)
              ),
              value = swallowException(Some(JsonSupport.toJson(Serializer.ROW.from(bytes)))),
              error = error
            )
        }
    )

  private[this] val zookeeperDefinition = ServiceDefinition(
    imageName = ZookeeperApi.IMAGE_NAME_DEFAULT,
    settingDefinitions = ZookeeperApi.DEFINITIONS,
    classInfos = Seq.empty
  )

  private[this] val brokerDefinition = ServiceDefinition(
    imageName = BrokerApi.IMAGE_NAME_DEFAULT,
    settingDefinitions = BrokerApi.DEFINITIONS,
    classInfos = Seq(
      ClassInfo(
        className = "N/A",
        classType = ClassType.TOPIC,
        settingDefinitions = TopicApi.DEFINITIONS
      )
    )
  )

  private[this] val shabondiDefinition = ServiceDefinition(
    imageName = ShabondiApi.IMAGE_NAME_DEFAULT,
    settingDefinitions = ShabondiDefinitions.basicDefinitions,
    classInfos = Seq(
      ClassInfo(ClassType.SOURCE, ShabondiApi.SHABONDI_SOURCE_CLASS_NAME, ShabondiDefinitions.sourceDefinitions),
      ClassInfo(ClassType.SINK, ShabondiApi.SHABONDI_SINK_CLASS_NAME, ShabondiDefinitions.sinkDefinitions)
    )
  )

  def apply(mode: Mode, k8sUrls: Option[K8sUrls])(
    implicit brokerCollie: BrokerCollie,
    dataStore: DataStore,
    serviceCollie: ServiceCollie,
    workerCollie: WorkerCollie,
    objectChecker: DataChecker,
    executionContext: ExecutionContext
  ): server.Route = pathPrefix(KIND) {
    path(RDB_PREFIX | "rdb") {
      post {
        entity(as[RdbQuery]) { query =>
          complete(both(query.workerClusterKey) { (_, connectorAdmin, _, topicAdmin) =>
            connectorAdmin match {
              case _: FakeConnectorAdmin =>
                val client = DatabaseClient.builder.url(query.url).user(query.user).password(query.password).build
                try Future.successful(
                  RdbInfo(
                    name = client.databaseType,
                    tables = client.tableQuery
                      .catalog(query.catalogPattern)
                      .schema(query.schemaPattern)
                      .tableName(query.tableName)
                      .execute()
                  )
                )
                finally client.close()
              case _ =>
                rdbInfo(
                  connectorAdmin,
                  topicAdmin,
                  query
                )
            }
          })
        }
      }
    } ~ path((TopicApi.PREFIX | TopicApi.KIND) / Segment) { topicName =>
      post {
        parameters(
          (
            GROUP_KEY ? GROUP_DEFAULT,
            TOPIC_TIMEOUT_KEY.as[Long] ? TOPIC_TIMEOUT_DEFAULT.toMillis,
            TOPIC_LIMIT_KEY.as[Int] ? TOPIC_LIMIT_DEFAULT
          )
        ) { (group, timeoutMs, limit) =>
          val topicKey = TopicKey.of(group, topicName)
          if (limit <= 0)
            throw DeserializationException(
              s"the limit must be bigger than zero. actual:$limit",
              fieldNames = List(TOPIC_LIMIT_KEY)
            )
          complete(
            objectChecker.checkList
              .topic(topicKey, DataCondition.RUNNING)
              .check()
              .map(_.topicInfos.head._1.brokerClusterKey)
              .flatMap(
                brokerClusterKey =>
                  objectChecker.checkList
                    .brokerCluster(brokerClusterKey, DataCondition.RUNNING)
                    .check()
                    .map(_.runningBrokers.head.connectionProps)
              )
              .map { connectionProps =>
                serviceCollie match {
                  // no true service so fake data
                  case s: FakeServiceCollie if !s.embedded =>
                    TopicData(
                      Seq(
                        Message(
                          partition = 0,
                          offset = 0,
                          sourceClass = None,
                          sourceKey = None,
                          value = Some(
                            JsObject(
                              Map(
                                "a" -> JsString("b"),
                                "b" -> JsNumber(123),
                                "c" -> JsArray(Vector(JsString("c"), JsString("d"), JsString("e"))),
                                "d" -> JsObject(Map("a" -> JsString("aaa"))),
                                "e" -> JsTrue
                              )
                            )
                          ),
                          error = None
                        )
                      )
                    )
                  case _ =>
                    val consumer = Consumer
                      .builder()
                      .connectionProps(connectionProps)
                      .build()
                    try {
                      val endTime = CommonUtils.current() + timeoutMs
                      consumer.assignments(
                        consumer
                          .endOffsets()
                          .asScala
                          .filter {
                            case (tp, _) =>
                              tp.topicKey() == topicKey
                          }
                          .map {
                            case (tp, offset) =>
                              tp -> lang.Long.valueOf(offset - limit)
                          }
                          .toMap
                          .asJava
                      )
                      topicData(
                        consumer
                        // even if the timeout reach the limit, we still give a last try :)
                          .poll(java.time.Duration.ofMillis(Math.max(1000L, endTime - CommonUtils.current())), limit)
                          .asScala
                          .slice(0, limit)
                          .toSeq
                      )
                    } finally Releasable.close(consumer)
                }
              }
          )
        }
      }
    } ~ path(FileInfoApi.PREFIX | FileInfoApi.KIND) {
      FileInfoRoute.routeOfUploadingFile(urlMaker = _ => None, storeOption = None)
    } ~ path(oharastream.ohara.client.configurator.CONFIGURATOR_KIND) {
      complete(
        ConfiguratorInfo(
          versionInfo = ConfiguratorVersion(
            version = VersionUtils.VERSION,
            branch = VersionUtils.BRANCH,
            user = VersionUtils.USER,
            revision = VersionUtils.REVISION,
            date = VersionUtils.DATE
          ),
          mode = mode.toString,
          k8sUrls = k8sUrls
        )
      )
    } ~ pathPrefix(ConnectorApi.PREFIX | ConnectorApi.KIND) {
      path(Segment) { name =>
        parameters(GROUP_KEY ? GROUP_DEFAULT) { group =>
          complete(
            dataStore
              .value[ConnectorInfo](ObjectKey.of(group, name))
              .flatMap {
                c => dataStore.value[WorkerClusterInfo](c.workerClusterKey)
                  .flatMap(workerCollie.connectorAdmin)
                  .flatMap(_.connectorDefinitions())
                  .map(classDefinitions =>
                    ServiceDefinition(
                      imageName = WorkerApi.IMAGE_NAME_DEFAULT,
                      settingDefinitions = Seq.empty,
                      classInfos = classDefinitions.get(c.className).map(Seq(_)).getOrElse(Seq.empty)
                    ))
              }
          )
        }
      }
    } ~ pathPrefix(WorkerApi.PREFIX | WorkerApi.KIND) {
      path(Segment) { name =>
        parameters(GROUP_KEY ? GROUP_DEFAULT) { group =>
          complete(
            dataStore
              .value[WorkerClusterInfo](ObjectKey.of(group, name))
              .flatMap(workerCollie.connectorAdmin)
              .flatMap(_.connectorDefinitions())
              .recover {
                case _: Throwable => Map.empty
              }
              .map { classDefinitions =>
                ServiceDefinition(
                  imageName = WorkerApi.IMAGE_NAME_DEFAULT,
                  settingDefinitions = WorkerApi.DEFINITIONS,
                  classInfos = classDefinitions.values.toSeq
                )
              }
          )
        }
      } ~ pathEnd {
        complete(
          ServiceDefinition(
            imageName = WorkerApi.IMAGE_NAME_DEFAULT,
            settingDefinitions = WorkerApi.DEFINITIONS,
            classInfos = Seq.empty
          )
        )
      }
    } ~ pathPrefix(BrokerApi.PREFIX | BrokerApi.KIND) {
      path(Segment) { name =>
        parameters(GROUP_KEY ? GROUP_DEFAULT) { group =>
          complete(
            dataStore
              .value[BrokerClusterInfo](ObjectKey.of(group, name))
              .map(_ => brokerDefinition)
          )
        }
      } ~ pathEnd {
        complete(brokerDefinition)
      }
    } ~ pathPrefix(ZookeeperApi.PREFIX | ZookeeperApi.KIND) {
      path(Segment) { name =>
        parameters(GROUP_KEY ? GROUP_DEFAULT) { group =>
          complete(
            dataStore
              .value[ZookeeperClusterInfo](ObjectKey.of(group, name))
              .map(_ => zookeeperDefinition)
          )
        }
      } ~ pathEnd {
        complete(zookeeperDefinition)
      }
    } ~ pathPrefix(StreamApi.PREFIX | StreamApi.KIND) {
      path(Segment) { name =>
        parameters(GROUP_KEY ? GROUP_DEFAULT) { group =>
          complete(
            dataStore
              .value[StreamClusterInfo](ObjectKey.of(group, name))
              .map(_.jarKey)
              .flatMap(dataStore.value[FileInfo])
              .map(file => Seq(file.url.get))
              .flatMap(serviceCollie.fileContent)
              .recover {
                case _: Throwable => FileContent.empty
              }
              .map(
                fileContent =>
                  ServiceDefinition(
                    imageName = StreamApi.IMAGE_NAME_DEFAULT,
                    settingDefinitions = StreamDefUtils.DEFAULT.values().asScala.toSeq,
                    classInfos = fileContent.streamClassInfos
                  )
              )
          )
        }
      } ~ pathEnd {
        complete(
          ServiceDefinition(
            imageName = StreamApi.IMAGE_NAME_DEFAULT,
            settingDefinitions = StreamDefUtils.DEFAULT.values().asScala.toSeq,
            classInfos = Seq.empty
          )
        )
      }
    } ~ pathPrefix(ShabondiApi.PREFIX | ShabondiApi.KIND) {
      path(Segment) { _ =>
        parameters(GROUP_KEY ? GROUP_DEFAULT) { _ =>
          complete(shabondiDefinition)
        }
      } ~ pathEnd {
        complete(shabondiDefinition)
      }
    }
  }

  /**
    * create a connector to query the DB.
    * Noted: we don't query the db via Configurator since the connection to DB requires specific drvier and it is not
    * available on Configurator. By contrast, the worker cluster which will be used to run Connector should have been
    * deployed the driver so we use our specific connector to query DB.
    * @return rdb information
    */
  private def rdbInfo(connectorAdmin: ConnectorAdmin, topicAdmin: TopicAdmin, request: RdbQuery)(
    implicit executionContext: ExecutionContext
  ): Future[RdbInfo] = {
    val requestId: String = CommonUtils.randomString()
    val connectorKey      = ConnectorKey.of(CommonUtils.randomString(5), s"Validator-${CommonUtils.randomString()}")
    connectorAdmin
      .connectorCreator()
      .connectorKey(connectorKey)
      .className("oharastream.ohara.connector.validation.Validator")
      .numberOfTasks(1)
      .topicKey(InspectApi.INTERNAL_TOPIC_KEY)
      .settings(
        Map(
          InspectApi.SETTINGS_KEY -> JsObject(InspectApi.RDB_QUERY_FORMAT.write(request).asJsObject.fields.filter {
            case (_, value) =>
              value match {
                case JsNull => false
                case _      => true
              }
          }).toString(),
          InspectApi.REQUEST_ID -> requestId,
          InspectApi.TARGET_KEY -> InspectApi.RDB_PREFIX
        )
      )
      .threadPool(executionContext)
      .create()
      .map { _ =>
        // TODO: receiving all messages may be expensive...by chia
        val client = Consumer
          .builder()
          .connectionProps(topicAdmin.connectionProps)
          .offsetFromBegin()
          .topicKey(InspectApi.INTERNAL_TOPIC_KEY)
          .keySerializer(Serializer.STRING)
          .valueSerializer(Serializer.OBJECT)
          .build()

        try client
          .poll(java.time.Duration.ofMillis(30 * 1000), 1)
          .asScala
          .filter(_.key().isPresent)
          .filter(_.key().get.equals(requestId))
          .filter(_.value().isPresent)
          .map(_.value().get())
          .filter {
            case _: RdbInfo => true
            case e: ErrorApi.Error =>
              throw new IllegalArgumentException(e.message)
            case _ => false
          }
          .map(_.asInstanceOf[RdbInfo])
          .head
        finally Releasable.close(client)
      }
      .flatMap(r => connectorAdmin.delete(connectorKey).map(_ => r))
  }
}
