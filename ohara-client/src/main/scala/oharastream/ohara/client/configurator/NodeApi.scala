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

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}
object NodeApi {
  val KIND: String   = SettingDef.Reference.NODE.name().toLowerCase
  val PREFIX: String = "nodes"
  // We use the hostname field as "spec.hostname" label in k8s, which has a limit length <= 63
  // also see https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
  val LIMIT_OF_HOSTNAME_LENGTH: Int = 63

  /**
    * node does not support group. However, we are in group world and there are many cases of inputting key (group, name)
    * to access resource. This method used to generate key for hostname of node.
    * @param hostname hostname
    * @return object key
    */
  def key(hostname: String): ObjectKey = ObjectKey.of(GROUP_DEFAULT, hostname)

  val ZOOKEEPER_SERVICE_NAME: String    = "zookeeper"
  val BROKER_SERVICE_NAME: String       = "broker"
  val WORKER_SERVICE_NAME: String       = "connect-worker"
  val STREAM_SERVICE_NAME: String       = "stream"
  val CONFIGURATOR_SERVICE_NAME: String = "configurator"

  case class Updating(
    port: Option[Int],
    user: Option[String],
    password: Option[String],
    override val tags: Option[Map[String, JsValue]]
  ) extends BasicUpdating {
    override def raw: Map[String, JsValue] = UPDATING_FORMAT.write(this).asJsObject.fields
  }
  implicit val UPDATING_FORMAT: RootJsonFormat[Updating] =
    JsonRefiner.builder[Updating].format(jsonFormat4(Updating)).requireConnectionPort("port").build

  case class Creation(
    hostname: String,
    port: Int,
    user: String,
    password: String,
    override val tags: Map[String, JsValue]
  ) extends oharastream.ohara.client.configurator.BasicCreation {
    override def group: String = GROUP_DEFAULT
    override def name: String  = hostname

    override def raw: Map[String, JsValue] = CREATION_FORMAT.write(this).asJsObject.fields
  }
  implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    JsonRefiner
      .builder[Creation]
      .format(jsonFormat5(Creation))
      // default implementation of node is ssh, we use "default ssh port" here
      .nullToInt("port", 22)
      .requireConnectionPort("port")
      .stringRestriction("hostname", SettingDef.HOSTNAME_REGEX)
      .nullToEmptyObject(TAGS_KEY)
      .build

  case class NodeService(name: String, clusterKeys: Seq[ObjectKey])
  implicit val NODE_SERVICE_FORMAT: RootJsonFormat[NodeService] = jsonFormat2(NodeService)

  case class Resource(name: String, value: Double, unit: String, used: Option[Double])
  object Resource {
    /**
      * generate a resource based on cores if the number of core is large than 1. Otherwise, the size is in core.
      * @param cores cores
      * @param used used
      * @return memory resource
      */
    def cpu(cores: Int, used: Option[Double]): Resource = Resource(
      name = "CPU",
      value = cores,
      unit = if (cores > 1) "cores" else "core",
      used = used
    )

    /**
      * generate a resource based on MB if the input value is large than MB. Otherwise, the size is in bytes.
      * @param bytes bytes
      * @param used used
      * @return memory resource
      */
    def memory(bytes: Long, used: Option[Double]): Resource =
      if (bytes < 1024)
        Resource(
          name = "Memory",
          value = bytes.toDouble,
          unit = "bytes",
          used = used
        )
      else if (bytes < 1024 * 1024)
        Resource(
          name = "Memory",
          value = bytes / 1024f,
          unit = "KB",
          used = used
        )
      else if (bytes < 1024 * 1024 * 1024)
        Resource(
          name = "Memory",
          value = bytes / 1024f / 1024f,
          unit = "MB",
          used = used
        )
      else
        Resource(
          name = "Memory",
          value = bytes / 1024f / 1024f / 1024f,
          unit = "GB",
          used = used
        )
  }
  implicit val RESOURCE_FORMAT: RootJsonFormat[Resource] = jsonFormat4(Resource.apply)

  sealed abstract class State
  object State extends oharastream.ohara.client.Enum[State] {
    case object AVAILABLE   extends State
    case object UNAVAILABLE extends State
  }
  implicit val STATE_FORMAT: RootJsonFormat[State] = new RootJsonFormat[State] {
    override def write(obj: State): JsValue = JsString(obj.toString)

    override def read(json: JsValue): State = State.forName(json.convertTo[String])
  }

  /**
    * NOTED: the field "services" is filled at runtime. If you are in testing, it is ok to assign empty to it.
    */
  case class Node(
    hostname: String,
    port: Int,
    user: String,
    password: String,
    services: Seq[NodeService],
    state: State,
    error: Option[String],
    override val lastModified: Long,
    resources: Seq[Resource],
    override val tags: Map[String, JsValue]
  ) extends Data {
    // Node does not support to define group
    override def group: String             = GROUP_DEFAULT
    override def name: String              = hostname
    override def kind: String              = KIND
    override def raw: Map[String, JsValue] = NODE_FORMAT.write(this).asJsObject.fields
  }

  object Node {
    /**
      * create a node with only hostname. It means this node is illegal to ssh mode.
      * @param hostname hostname
      * @return node
      */
    def apply(hostname: String, user: String, password: String): Node = apply(
      hostname = hostname,
      port = 22,
      user = user,
      password = password
    )

    def apply(hostname: String, port: Int, user: String, password: String): Node = Node(
      hostname = hostname,
      port = port,
      user = user,
      password = password,
      services = Seq.empty,
      state = State.AVAILABLE,
      error = None,
      lastModified = CommonUtils.current(),
      resources = Seq.empty,
      tags = Map.empty
    )
  }

  implicit val NODE_FORMAT: RootJsonFormat[Node] = jsonFormat10(Node.apply)

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  trait Request {
    /**
      * a specific setter that user can generate a request according to a existent node.
      * Noted that not all fields are copy to server. the included fields are shown below.
      * 1) hostname
      * 2) port
      * 3) user
      * 4) password
      * 5) tags
      * @param node node info
      * @return request
      */
    def node(node: Node): Request

    /**
      * set the node name
      * @param nodeName node name
      * @return this request builder
      */
    def nodeName(nodeName: String): Request

    @Optional("it is ignorable if you are going to send update request")
    def port(port: Int): Request

    @Optional("it is ignorable if you are going to send update request")
    def user(user: String): Request

    @Optional("it is ignorable if you are going to send update request")
    def password(password: String): Request

    @Optional("default value is empty array")
    def tags(tags: Map[String, JsValue]): Request

    /**
      * Retrieve the inner object of request payload. Noted, it throw unchecked exception if you haven't filled all required fields
      * @return the payload of creation
      */
    @VisibleForTesting
    private[configurator] def creation: Creation

    /**
      * Retrieve the inner object of request payload. Noted, it throw unchecked exception if you haven't filled all required fields
      * @return the payload of creation
      */
    @VisibleForTesting
    private[configurator] def updating: Updating

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[Node]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[Node]
  }

  class Access private[configurator]
      extends oharastream.ohara.client.configurator.Access[Creation, Updating, Node](PREFIX) {
    def request: Request = new Request {
      private[this] var nodeName: String           = _
      private[this] var port: Option[Int]          = None
      private[this] var user: String               = _
      private[this] var password: String           = _
      private[this] var tags: Map[String, JsValue] = _

      override def node(node: Node): Request = {
        this.nodeName = node.hostname
        this.port = Some(node.port)
        this.user = node.user
        this.password = node.password
        this.tags = node.tags
        this
      }

      override def nodeName(nodeName: String): Request = {
        this.nodeName = CommonUtils.requireNonEmpty(nodeName)
        this
      }
      override def port(port: Int): Request = {
        this.port = Some(CommonUtils.requireConnectionPort(port))
        this
      }
      override def user(user: String): Request = {
        this.user = CommonUtils.requireNonEmpty(user)
        this
      }
      override def password(password: String): Request = {
        this.password = CommonUtils.requireNonEmpty(password)
        this
      }

      override def tags(tags: Map[String, JsValue]): Request = {
        this.tags = Objects.requireNonNull(tags)
        this
      }

      override private[configurator] def creation: Creation =
        // auto-complete the creation via our refiner
        CREATION_FORMAT.read(
          CREATION_FORMAT.write(
            Creation(
              hostname = CommonUtils.requireNonEmpty(nodeName),
              user = CommonUtils.requireNonEmpty(user),
              password = CommonUtils.requireNonEmpty(password),
              port = CommonUtils.requireConnectionPort(port.getOrElse(22)),
              tags = if (tags == null) Map.empty else tags
            )
          )
        )

      override private[configurator] def updating: Updating =
        // auto-complete the updating via our refiner
        UPDATING_FORMAT.read(
          UPDATING_FORMAT.write(
            Updating(
              port = port.map(CommonUtils.requireConnectionPort),
              user = Option(user).map(CommonUtils.requireNonEmpty),
              password = Option(password).map(CommonUtils.requireNonEmpty),
              tags = Option(tags)
            )
          )
        )

      override def create()(implicit executionContext: ExecutionContext): Future[Node] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[Node] =
        put(ObjectKey.of(GROUP_DEFAULT, nodeName), updating)
    }
  }

  def access: Access = new Access
}
