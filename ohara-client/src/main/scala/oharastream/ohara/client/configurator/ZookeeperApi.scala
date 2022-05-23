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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.ClusterAccess.Query
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsValue, RootJsonFormat}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
object ZookeeperApi {
  val KIND: String   = SettingDef.Reference.ZOOKEEPER.name().toLowerCase
  val PREFIX: String = "zookeepers"

  /**
    * the default docker image used to run containers of worker cluster
    */
  val IMAGE_NAME_DEFAULT: String = s"ghcr.io/skiptests/ohara/zookeeper:${VersionUtils.VERSION}"

  val PEER_PORT_KEY          = "peerPort"
  val ELECTION_PORT_KEY      = "electionPort"
  val TICK_TIME_KEY          = "tickTime"
  val INIT_LIMIT_KEY         = "initLimit"
  val SYNC_LIMIT_KEY         = "syncLimit"
  val DATA_DIR_KEY           = "dataDir"
  val CONNECTION_TIMEOUT_KEY = "zookeeper.connection.timeout.ms"

  val DEFINITIONS: Seq[SettingDef] = DefinitionCollector()
    .addFollowupTo("core")
    .group()
    .name()
    .imageName(IMAGE_NAME_DEFAULT)
    .nodeNames()
    .routes()
    .tags()
    .addFollowupTo("performance")
    .definition(
      _.key(INIT_LIMIT_KEY)
        .documentation("timeout to connect to leader")
        .positiveNumber(10)
    )
    .definition(
      _.key(TICK_TIME_KEY)
        .documentation("basic time unit in zookeeper")
        .positiveNumber(2000)
    )
    .definition(
      _.key(SYNC_LIMIT_KEY)
        .documentation("the out-of-date of a sever from leader")
        .positiveNumber(5)
    )
    .definition(
      _.key(DATA_DIR_KEY)
        .documentation("the volume used to store zookeeper data")
        .optional(SettingDef.Type.OBJECT_KEY)
        .reference(SettingDef.Reference.VOLUME)
    )
    .definition(
      _.key(CONNECTION_TIMEOUT_KEY)
        .documentation("zookeeper connection timeout")
        .optional(java.time.Duration.ofMillis(10 * 1000))
    )
    .initHeap()
    .maxHeap()
    .addFollowupTo("public")
    .definition(
      _.key(PEER_PORT_KEY)
        .documentation("the port exposed to each quorum")
        .bindingPortWithRandomDefault()
    )
    .definition(
      _.key(ELECTION_PORT_KEY)
        .documentation("quorum leader election port")
        .bindingPortWithRandomDefault()
    )
    .clientPort()
    .jmxPort()
    .result

  final class Creation(val raw: Map[String, JsValue]) extends ClusterCreation {
    /**
      * reuse the parser from Update.
      *
      * @return update
      */
    private[this] implicit def update(raw: Map[String, JsValue]): Updating = new Updating(raw)
    override def ports: Set[Int]                                           = Set(clientPort, peerPort, electionPort, jmxPort)
    def clientPort: Int                                                    = raw.clientPort.get
    def peerPort: Int                                                      = raw.peerPort.get
    def electionPort: Int                                                  = raw.electionPort.get
    def tickTime: Int                                                      = raw.tickTime.get
    def initLimit: Int                                                     = raw.initLimit.get
    def syncLimit: Int                                                     = raw.syncLimit.get
    def connectionTimeout: Duration                                        = raw.connectionTimeout.get
    def dataFolder: String                                                 = "/tmp/zk_data"

    /**
      * @return the file containing zk quorum id. Noted that id file must be in data folder (see above)
      */
    def idFile: String = "/tmp/zk_data/myid"
    override def volumeMaps: Map[ObjectKey, String] =
      Map(DATA_DIR_KEY -> dataFolder)
        .flatMap {
          case (key, localPath) =>
            raw.get(key).map(_ -> localPath)
        }
        .map {
          case (js, localPath) =>
            js.convertTo[ObjectKey] -> localPath
        }
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    rulesOfCreation[Creation](
      new RootJsonFormat[Creation] {
        override def write(obj: Creation): JsValue = JsObject(obj.raw)
        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
      },
      DEFINITIONS
    )

  final class Updating(val raw: Map[String, JsValue]) extends ClusterUpdating {
    def clientPort: Option[Int]   = raw.get(CLIENT_PORT_KEY).map(_.convertTo[Int])
    def peerPort: Option[Int]     = raw.get(PEER_PORT_KEY).map(_.convertTo[Int])
    def electionPort: Option[Int] = raw.get(ELECTION_PORT_KEY).map(_.convertTo[Int])
    def tickTime: Option[Int]     = raw.get(TICK_TIME_KEY).map(_.convertTo[Int])
    def initLimit: Option[Int]    = raw.get(INIT_LIMIT_KEY).map(_.convertTo[Int])
    def syncLimit: Option[Int]    = raw.get(SYNC_LIMIT_KEY).map(_.convertTo[Int])
    def connectionTimeout: Option[Duration] =
      raw
        .get(CONNECTION_TIMEOUT_KEY)
        .map(_.convertTo[String])
        .map(CommonUtils.toDuration)
        .map(d => Duration(d.toMillis, TimeUnit.MILLISECONDS))
  }

  implicit val UPDATING_FORMAT: JsonRefiner[Updating] =
    rulesOfUpdating[Updating](
      new RootJsonFormat[Updating] {
        override def write(obj: Updating): JsValue = JsObject(obj.raw)
        override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)
      }
    )

  final case class ZookeeperClusterInfo private[ZookeeperApi] (
    settings: Map[String, JsValue],
    aliveNodes: Set[String],
    override val lastModified: Long,
    state: Option[ClusterState],
    error: Option[String]
  ) extends ClusterInfo {
    /**
      * reuse the parser from Creation.
      *
      * @return creation
      */
    private[this] implicit def creation(raw: Map[String, JsValue]): Creation = new Creation(raw)
    override def kind: String                                                = KIND
    override def ports: Set[Int]                                             = Set(clientPort, peerPort, electionPort, jmxPort)
    def clientPort: Int                                                      = settings.clientPort
    def peerPort: Int                                                        = settings.peerPort
    def electionPort: Int                                                    = settings.electionPort
    def tickTime: Int                                                        = settings.tickTime
    def initLimit: Int                                                       = settings.initLimit
    def syncLimit: Int                                                       = settings.syncLimit
    def dataDir: String                                                      = settings.dataFolder

    override def raw: Map[String, JsValue] = ZOOKEEPER_CLUSTER_INFO_FORMAT.write(this).asJsObject.fields

    override def volumeMaps: Map[ObjectKey, String] = settings.volumeMaps
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val ZOOKEEPER_CLUSTER_INFO_FORMAT: JsonRefiner[ZookeeperClusterInfo] =
    JsonRefiner(new RootJsonFormat[ZookeeperClusterInfo] {
      private[this] val format                               = jsonFormat5(ZookeeperClusterInfo)
      override def read(json: JsValue): ZookeeperClusterInfo = format.read(extractSetting(json.asJsObject))
      override def write(obj: ZookeeperClusterInfo): JsValue = flattenSettings(format.write(obj).asJsObject)
    })

  /**
    * used to generate the payload and url for POST/PUT request.
    * this request is extended by collie also so it is public than sealed.
    */
  trait Request extends ClusterRequest {
    @Optional("the default port is random")
    def jmxPort(jmxPort: Int): Request.this.type =
      setting(JMX_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(jmxPort)))

    def clientPort(clientPort: Int): Request.this.type =
      setting(CLIENT_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(clientPort)))

    @Optional("the default port is random")
    def peerPort(peerPort: Int): Request.this.type =
      setting(PEER_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(peerPort)))

    @Optional("the default port is random")
    def electionPort(electionPort: Int): Request.this.type =
      setting(ELECTION_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(electionPort)))

    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): Request.this.type = setting(TAGS_KEY, JsObject(tags))

    @Optional("default is no volume mounted on data folder so all data is in container")
    def dataDir(volumeKey: ObjectKey): Request.this.type = volume(DATA_DIR_KEY, volumeKey)

    /**
      * zookeeper information creation.
      * Here we open the access for reusing the creation to other module
      *
      * @return the payload of create
      */
    final def creation: Creation =
      // auto-complete the creation via our refiner
      CREATION_FORMAT.read(CREATION_FORMAT.write(new Creation(settings.toMap)))

    /**
      * for testing only
      * @return the payload of update
      */
    private[configurator] final def updating: Updating =
      // auto-complete the update via our refiner
      UPDATING_FORMAT.read(UPDATING_FORMAT.write(new Updating(settings.toMap)))
  }

  /**
    * similar to Request but it has execution methods.
    *
    */
  sealed trait ExecutableRequest extends Request {
    def create()(implicit executionContext: ExecutionContext): Future[ZookeeperClusterInfo]
    def update()(implicit executionContext: ExecutionContext): Future[ZookeeperClusterInfo]
  }

  final class Access private[ZookeeperApi] extends ClusterAccess[Creation, Updating, ZookeeperClusterInfo](PREFIX) {
    override def query: Query[ZookeeperClusterInfo] = new Query[ZookeeperClusterInfo] {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[ZookeeperClusterInfo]] = list(request)
    }

    def request: ExecutableRequest = new ExecutableRequest {
      override def create()(implicit executionContext: ExecutionContext): Future[ZookeeperClusterInfo] = post(creation)

      override def update()(implicit executionContext: ExecutionContext): Future[ZookeeperClusterInfo] =
        put(key, updating)
    }
  }

  def access: Access = new Access
}
