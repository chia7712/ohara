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
import java.util.Objects

import oharastream.ohara.client.Enum
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.data.Column
import oharastream.ohara.common.setting._
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json._
import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsArray, JsObject, JsString, JsValue, RootJsonFormat, _}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

object ConnectorApi {
  val KIND: String                                         = SettingDef.Reference.CONNECTOR.name().toLowerCase
  val PREFIX: String                                       = "connectors"
  private[configurator] val WORKER_CLUSTER_KEY_KEY: String = ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key()
  private[this] val NUMBER_OF_TASKS_KEY: String            = ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key()
  private[this] val TOPIC_KEYS_KEY: String                 = ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key()
  @VisibleForTesting
  private[ohara] val CONNECTOR_CLASS_KEY: String = ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key()
  @VisibleForTesting
  private[configurator] val COLUMNS_KEY: String = ConnectorDefUtils.COLUMNS_DEFINITION.key()
  @VisibleForTesting
  private[configurator] val CONNECTOR_KEY_KEY: String = ConnectorDefUtils.CONNECTOR_KEY_DEFINITION.key()
  private[this] val PARTITIONER_CLASS_KEY: String     = ConnectorDefUtils.PARTITIONER_CLASS_DEFINITION.key()

  /**
    * The name is a part of "Restful APIs" so "DON'T" change it arbitrarily
    */
  // Make this class to be serializable since it's stored in configurator
  abstract sealed class State(val name: String) extends Serializable
  object State extends Enum[State] {
    case object UNASSIGNED extends State("UNASSIGNED")
    case object RUNNING    extends State("RUNNING")
    case object PAUSED     extends State("PAUSED")
    case object FAILED     extends State("FAILED")
    case object DESTROYED  extends State("DESTROYED")
  }

  implicit val CONNECTOR_STATE_FORMAT: RootJsonFormat[State] =
    new RootJsonFormat[State] {
      override def write(obj: State): JsValue = JsString(obj.name)
      override def read(json: JsValue): State =
        State.forName(json.convertTo[String])
    }

  final class Creation(val raw: Map[String, JsValue]) extends oharastream.ohara.client.configurator.BasicCreation {
    override def key: ConnectorKey = ConnectorKey.of(group, name)

    /**
      * Convert all json value to plain string. It keeps the json format but all stuff are in string.
      */
    def plain: Map[String, String] = raw.map {
      case (k, v) =>
        k -> (v match {
          case JsString(value) => value
          case _               => v.toString()
        })
    }

    private[this] implicit def update(raw: Map[String, JsValue]): Updating = new Updating(raw)
    def className: String                                                  = raw.className.get
    def columns: Seq[Column]                                               = raw.columns.get
    def numberOfTasks: Int                                                 = raw.numberOfTasks.get
    def workerClusterKey: ObjectKey                                        = raw.workerClusterKey.get
    def topicKeys: Set[TopicKey]                                           = raw.topicKeys.get
    def partitionClass: String                                             = raw.partitionerClass.get
  }

  val DEFINITIONS: Seq[SettingDef] = ConnectorDefUtils.DEFAULT.asScala.values.toSeq

  implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    // this object is open to user define the (group, name) in UI, we need to handle the key rules
    limitsOfKey[Creation]
      .format(new RootJsonFormat[Creation] {
        override def write(obj: Creation): JsValue = JsObject(obj.raw)
        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
      })
      .definitions(DEFINITIONS)
      .valuesChecker(
        Set(COLUMNS_KEY),
        _(COLUMNS_KEY) match {
          case v: JsArray if v.elements.nonEmpty =>
            try {
              val columns = PropGroup.ofJson(v.toString()).toColumns.asScala
              // name can't be empty
              if (columns.exists(_.name().isEmpty))
                throw DeserializationException(msg = s"name can't be empty", fieldNames = List("name"))
              // newName can't be empty
              if (columns.exists(_.newName().isEmpty))
                throw DeserializationException(msg = s"newName can't be empty", fieldNames = List("newName"))
              // order can't be negative number
              if (columns.exists(_.order() < 0))
                throw DeserializationException(msg = s"order can't be negative number", fieldNames = List("order"))
              // order can't be duplicate
              if (columns.map(_.order).toSet.size != columns.size)
                throw DeserializationException(
                  msg = s"duplicate order:${columns.map(_.order)}",
                  fieldNames = List("order")
                )
            } catch {
              case e: DeserializationException => throw e
              case other: Throwable =>
                throw DeserializationException(
                  msg = other.getMessage,
                  cause = other,
                  fieldNames = List(COLUMNS_KEY)
                )
            }
          case _ => // do nothing
        }
      )
      .ignoreKeys(RUNTIME_KEYS)
      .build

  final class Updating(val raw: Map[String, JsValue]) extends BasicUpdating {
    def className: Option[String] = raw.get(CONNECTOR_CLASS_KEY).map(_.convertTo[String])

    def columns: Option[Seq[Column]] =
      raw.get(COLUMNS_KEY).map(s => PropGroup.ofJson(s.toString).toColumns.asScala.toSeq)
    def numberOfTasks: Option[Int] = raw.get(NUMBER_OF_TASKS_KEY).map(_.convertTo[Int])

    def workerClusterKey: Option[ObjectKey] = raw.get(WORKER_CLUSTER_KEY_KEY).map(_.convertTo[ObjectKey])

    def topicKeys: Option[Set[TopicKey]] =
      raw.get(TOPIC_KEYS_KEY).map(_.convertTo[Set[TopicKey]])

    def partitionerClass: Option[String] = raw.get(PARTITIONER_CLASS_KEY).map(_.convertTo[String])
  }

  implicit val UPDATING_FORMAT: RootJsonFormat[Updating] = JsonRefiner
    .builder[Updating]
    .format(new RootJsonFormat[Updating] {
      override def write(obj: Updating): JsValue = JsObject(obj.raw)
      override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)
    })
    .valuesChecker(
      Set(COLUMNS_KEY),
      _(COLUMNS_KEY) match {
        case v: JsArray => CREATION_FORMAT.check(COLUMNS_KEY, v)
        case _          => // do nothing
      }
    )
    .ignoreKeys(RUNTIME_KEYS)
    .build

  import MetricsApi._

  case class Status(state: State, nodeName: String, error: Option[String], coordinator: Boolean)
  implicit val STATUS_FORMAT: RootJsonFormat[Status] = jsonFormat4(Status)

  /**
    * this is what we store in configurator
    */
  final case class ConnectorInfo(
    settings: Map[String, JsValue],
    state: Option[State],
    aliveNodes: Set[String],
    error: Option[String],
    tasksStatus: Seq[Status],
    nodeMetrics: Map[String, Metrics],
    override val lastModified: Long
  ) extends Data
      with Metricsable {
    override def key: ConnectorKey         = settings.key
    override def kind: String              = KIND
    override def raw: Map[String, JsValue] = CONNECTOR_INFO_FORMAT.write(this).asJsObject.fields

    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = new Creation(settings)

    /**
      * Convert all json value to plain string. It keeps the json format but all stuff are in string.
      */
    def plain: Map[String, String]  = settings.plain
    def className: String           = settings.className
    def columns: Seq[Column]        = settings.columns
    def numberOfTasks: Int          = settings.numberOfTasks
    def workerClusterKey: ObjectKey = settings.workerClusterKey
    def topicKeys: Set[TopicKey]    = settings.topicKeys
    def partitionClass: String      = settings.partitionClass
  }

  implicit val CONNECTOR_INFO_FORMAT: RootJsonFormat[ConnectorInfo] = JsonRefiner(new RootJsonFormat[ConnectorInfo] {
    private[this] val format                        = jsonFormat7(ConnectorInfo)
    override def read(json: JsValue): ConnectorInfo = format.read(extractSetting(json.asJsObject))
    override def write(obj: ConnectorInfo): JsValue = flattenSettings(format.write(obj).asJsObject)
  })

  /**
    * used to generate the payload and url for POST/PUT request.
    * This basic class is used to collect settings of connector. It is also used by validation so we extract the same behavior from Request.
    * We use private[configurator] instead of "sealed" since it is extendable to ValidationApi.
    */
  abstract class BasicRequest private[configurator] {
    protected[this] val settings: mutable.Map[String, JsValue] = mutable.Map()

    def key(key: ConnectorKey): BasicRequest.this.type = {
      group(key.group())
      name(key.name())
    }
    def group(group: String): BasicRequest.this.type =
      setting(GROUP_KEY, JsString(CommonUtils.requireNonEmpty(group)))
    def name(name: String): BasicRequest.this.type =
      setting(NAME_KEY, JsString(CommonUtils.requireNonEmpty(name)))

    def className(className: String): BasicRequest.this.type =
      setting(CONNECTOR_CLASS_KEY, JsString(CommonUtils.requireNonEmpty(className)))

    @Optional("Not all connectors demand this field. See connectors document for more details")
    def columns(columns: Seq[Column]): BasicRequest.this.type =
      setting(COLUMNS_KEY, PropGroup.ofColumns(columns.asJava).toJsonString.parseJson)

    @Optional(
      "You don't need to fill this field when update/create connector. But this filed is required in starting connector"
    )
    def topicKey(topicKey: TopicKey): BasicRequest.this.type = topicKeys(Set(Objects.requireNonNull(topicKey)))

    @Optional(
      "You don't need to fill this field when update/create connector. But this filed is required in starting connector"
    )
    def topicKeys(topicKeys: Set[TopicKey]): BasicRequest.this.type =
      setting(TOPIC_KEYS_KEY, TopicKey.toJsonString(topicKeys.asJava).parseJson)

    @Optional("default value is 1")
    def numberOfTasks(numberOfTasks: Int): BasicRequest.this.type =
      setting(NUMBER_OF_TASKS_KEY, JsNumber(CommonUtils.requirePositiveInt(numberOfTasks)))

    @Optional("server will match a worker cluster for you if the wk name is ignored")
    def workerClusterKey(workerClusterKey: ObjectKey): BasicRequest.this.type =
      setting(WORKER_CLUSTER_KEY_KEY, OBJECT_KEY_FORMAT.write(Objects.requireNonNull(workerClusterKey)))

    @Optional("extra settings for this connectors")
    def setting(key: String, value: JsValue): BasicRequest.this.type =
      settings(Map(CommonUtils.requireNonEmpty(key) -> Objects.requireNonNull(value)))

    @Optional("extra settings for this connectors")
    def settings(settings: Map[String, JsValue]): BasicRequest.this.type = {
      this.settings ++= Objects.requireNonNull(settings)
      this
    }

    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): BasicRequest.this.type =
      setting(TAGS_KEY, JsObject(Objects.requireNonNull(tags)))

    /**
      * generate the payload for request. It removes the ignored fields and keeping all value in json representation.
      * This method is exposed to sub classes since this generation is not friendly and hence we should reuse it as much as possible.
      * Noted, it throw unchecked exception if you haven't filled all required fields
      * @return creation object
      */
    final def creation: Creation =
      CREATION_FORMAT.read(CREATION_FORMAT.write(new Creation(settings.toMap)))

    private[configurator] final def updating: Updating =
      UPDATING_FORMAT.read(UPDATING_FORMAT.write(new Updating(settings.toMap)))
  }

  /**
    * The do-action methods are moved from BasicRequest to this one. Hence, ValidationApi ConnectorRequest does not have those weired methods
    */
  sealed abstract class Request extends BasicRequest {
    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[ConnectorInfo]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[ConnectorInfo]
  }

  sealed trait Query extends BasicQuery[ConnectorInfo] {
    def state(value: State): Query = setting("state", value.name)
    // TODO: there are a lot of settings which is worth of having parameters ... by chia
  }

  class Access private[configurator]
      extends oharastream.ohara.client.configurator.Access[Creation, Updating, ConnectorInfo](PREFIX) {
    /**
      * start to run a connector on worker cluster.
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def start(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, START_COMMAND)

    /**
      * stop and remove a running connector.
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def stop(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, STOP_COMMAND)

    /**
      * pause a running connector
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def pause(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, PAUSE_COMMAND)

    /**
      * resume a paused connector
      *
      * @param key connector's key
      * @return the configuration of connector
      */
    def resume(key: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, RESUME_COMMAND)

    def query: Query = new Query {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[ConnectorInfo]] = list(request)
    }

    def request: Request = new Request {
      override def create()(implicit executionContext: ExecutionContext): Future[ConnectorInfo] = post(creation)

      override def update()(implicit executionContext: ExecutionContext): Future[ConnectorInfo] =
        put(
          ConnectorKey.of(
            settings.get(GROUP_KEY).map(_.convertTo[String]).getOrElse(GROUP_DEFAULT),
            settings(NAME_KEY).convertTo[String]
          ),
          updating
        )
    }
  }

  def access: Access = new Access
}
