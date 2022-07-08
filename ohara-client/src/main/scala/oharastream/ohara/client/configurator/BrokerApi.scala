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

import oharastream.ohara.client.configurator.ClusterAccess.Query
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.SettingDef.{Reference, Type}
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}
object BrokerApi {
  val KIND: String   = SettingDef.Reference.BROKER.name().toLowerCase
  val PREFIX: String = "brokers"

  /**
    * the default docker image used to run containers of broker cluster
    */
  val IMAGE_NAME_DEFAULT: String = s"ghcr.io/skiptests/ohara/broker:${VersionUtils.VERSION}"

  val ZOOKEEPER_CLUSTER_KEY_KEY: String = "zookeeperClusterKey"

  val LOG_DIRS_KEY: String             = "log.dirs"
  val NUMBER_OF_PARTITIONS_KEY: String = "num.partitions"

  val NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY: String = "offsets.topic.replication.factor"

  val NUMBER_OF_REPLICATIONS_4_TRANSACTION_TOPIC_KEY: String = "transaction.state.log.replication.factor"

  val MIN_IN_SYNC_4_TOPIC_KEY: String = "min.insync.replicas"

  val MIN_IN_SYNC_4_TRANSACTION_TOPIC_KEY: String = "transaction.state.log.min.isr"

  val NUMBER_OF_NETWORK_THREADS_KEY: String = "num.network.threads"

  val NUMBER_OF_IO_THREADS_KEY: String = "num.io.threads"

  // KafkaConfig.QueuedMaxBytesProp
  val MAX_OF_POOL_MEMORY_BYTES: String = "queued.max.request.bytes"

  // KafkaConfig.SocketRequestMaxBytesProp
  val MAX_OF_REQUEST_MEMORY_BYTES: String = "socket.request.max.bytes"

  val JVM_PERFORMANCE_OPTIONS_KEY: String = "jvm.performance.options"

  val DEFINITIONS: Seq[SettingDef] = DefinitionCollector()
    .addFollowupTo("core")
    .group()
    .name()
    .imageName(IMAGE_NAME_DEFAULT)
    .nodeNames()
    .routes()
    .tags()
    .definition(
      _.key(ZOOKEEPER_CLUSTER_KEY_KEY)
        .documentation("the zookeeper cluster used to manage broker nodes")
        .required(Type.OBJECT_KEY)
        .reference(Reference.ZOOKEEPER)
    )
    .addFollowupTo("performance")
    .definition(
      _.key(LOG_DIRS_KEY)
        .documentation("the folder used to store data of broker")
        // broker service can take multiples folders to speedup the I/O
        .optional(SettingDef.Type.OBJECT_KEYS)
        .reference(SettingDef.Reference.VOLUME)
    )
    .definition(
      _.key(NUMBER_OF_PARTITIONS_KEY)
        .documentation("the number of partitions for all topics by default")
        .positiveNumber(1)
    )
    .definition(
      _.key(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY)
        .documentation("the number of replications for internal offset topic")
        .positiveNumber(1)
    )
    .definition(
      _.key(NUMBER_OF_REPLICATIONS_4_TRANSACTION_TOPIC_KEY)
        .documentation("the number of replications for internal transaction topic")
        .positiveNumber(1)
    )
    .definition(
      _.key(MIN_IN_SYNC_4_TOPIC_KEY)
        .documentation("the min in-sync for all topics by default")
        .positiveNumber(1)
    )
    .definition(
      _.key(MIN_IN_SYNC_4_TRANSACTION_TOPIC_KEY)
        .documentation("the min in-sync for internal transaction topic by default")
        .positiveNumber(1)
    )
    .definition(
      _.key(NUMBER_OF_NETWORK_THREADS_KEY)
        .documentation("the number of threads used to accept network requests")
        .positiveNumber(1)
    )
    .definition(
      _.key(NUMBER_OF_IO_THREADS_KEY)
        .documentation("the number of threads used to process network requests")
        .positiveNumber(1)
    )
    .definition(
      _.key(MAX_OF_POOL_MEMORY_BYTES)
        .documentation("Set the max memory usage of pool if you define a positive number")
        .optional(-1L)
    )
    .definition(
      _.key(MAX_OF_REQUEST_MEMORY_BYTES)
        .documentation("Set the max memory allocation of per request")
        .positiveNumber(100 * 1024 * 1024L)
    )
    .definition(
      _.key(JVM_PERFORMANCE_OPTIONS_KEY)
        .documentation("the performance configs for JVM. for example: GC and GC arguments")
        .optional(SettingDef.Type.STRING)
    )
    .initHeap()
    .maxHeap()
    .addFollowupTo("public")
    .clientPort()
    .jmxPort()
    .result

  val TOPIC_DEFINITION: TopicDefinition = TopicDefinition(TopicApi.DEFINITIONS)

  final class Creation(val raw: Map[String, JsValue]) extends ClusterCreation {
    /**
      * reuse the parser from Update.
      *
      * @return update
      */
    private[this] implicit def update(raw: Map[String, JsValue]): Updating = new Updating(raw)

    override def ports: Set[Int]       = Set(clientPort, jmxPort)
    def clientPort: Int                = raw.clientPort.get
    def zookeeperClusterKey: ObjectKey = raw.zookeeperClusterKey.get
    private[this] def logVolumeKeys: Set[ObjectKey] =
      raw
        .get(LOG_DIRS_KEY)
        .map(_.convertTo[Set[ObjectKey]])
        .getOrElse(Set.empty)

    /**
      * @param index dir index
      * @return dir path with fixed postfix
      */
    private[this] def logDir(index: Int) = s"/tmp/bk_data_$index"

    def dataFolders: String =
      if (logVolumeKeys.isEmpty) "/tmp/bk_data"
      else
        logVolumeKeys.zipWithIndex
          .map {
            case (_, index) => logDir(index)
          }
          .mkString(",")
    def numberOfPartitions: Int = raw.numberOfPartitions.get
    def numberOfReplications4OffsetsTopic: Int = raw.numberOfReplications4OffsetsTopic.get
    def numberOfReplications4TransactionTopic: Int = raw.numberOfReplications4TransactionTopic.get
    def minInSync4Topic: Int = raw.minInSync4Topic.get
    def minInSync4TransactionTopic: Int = raw.minInSync4TransactionTopic.get
    def numberOfNetworkThreads: Int           = raw.numberOfNetworkThreads.get
    def numberOfIoThreads: Int                = raw.numberOfIoThreads.get
    def maxOfPoolMemory: Long                 = raw.maxOfPoolMemory.get
    def maxOfRequestMemory: Long              = raw.maxOfRequestMemory.get
    def jvmPerformanceOptions: Option[String] = raw.jvmPerformanceOptions

    override def volumeMaps: Map[ObjectKey, String] =
      if (logVolumeKeys.isEmpty) Map.empty
      else
        logVolumeKeys.zipWithIndex.map {
          case (key, index) => key -> logDir(index)
        }.toMap
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
    def clientPort: Option[Int] = raw.get(CLIENT_PORT_KEY).map(_.convertTo[Int])

    def zookeeperClusterKey: Option[ObjectKey] =
      raw.get(ZOOKEEPER_CLUSTER_KEY_KEY).map(_.convertTo[ObjectKey])

    def numberOfPartitions: Option[Int] =
      raw.get(NUMBER_OF_PARTITIONS_KEY).map(_.convertTo[Int])

    def numberOfReplications4OffsetsTopic: Option[Int] =
      raw.get(NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY).map(_.convertTo[Int])

    def numberOfReplications4TransactionTopic: Option[Int] =
      raw.get(NUMBER_OF_REPLICATIONS_4_TRANSACTION_TOPIC_KEY).map(_.convertTo[Int])

    def minInSync4Topic: Option[Int] =
      raw.get(MIN_IN_SYNC_4_TOPIC_KEY).map(_.convertTo[Int])

    def minInSync4TransactionTopic: Option[Int] =
      raw.get(MIN_IN_SYNC_4_TRANSACTION_TOPIC_KEY).map(_.convertTo[Int])

    def numberOfNetworkThreads: Option[Int] =
      raw.get(NUMBER_OF_NETWORK_THREADS_KEY).map(_.convertTo[Int])

    def numberOfIoThreads: Option[Int] =
      raw.get(NUMBER_OF_IO_THREADS_KEY).map(_.convertTo[Int])

    def maxOfPoolMemory: Option[Long] =
      raw.get(MAX_OF_POOL_MEMORY_BYTES).map(_.convertTo[Long])

    def maxOfRequestMemory: Option[Long] =
      raw.get(MAX_OF_REQUEST_MEMORY_BYTES).map(_.convertTo[Long])

    def jvmPerformanceOptions: Option[String] =
      raw.get(JVM_PERFORMANCE_OPTIONS_KEY).map(_.convertTo[String])
  }

  implicit val UPDATING_FORMAT: JsonRefiner[Updating] =
    rulesOfUpdating[Updating](new RootJsonFormat[Updating] {
      override def write(obj: Updating): JsValue = JsObject(obj.raw)
      override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)
    })

  final case class TopicDefinition(settingDefinitions: Seq[SettingDef])

  implicit val TOPIC_DEFINITION_FORMAT: JsonRefiner[TopicDefinition] =
    JsonRefiner.builder[TopicDefinition].format(jsonFormat1(TopicDefinition)).build

  final case class BrokerClusterInfo private[BrokerApi] (
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
    override def ports: Set[Int]                                             = Set(clientPort, jmxPort)

    /**
      * the node names is not equal to "running" nodes. The connection props may reference to invalid nodes and the error
      * should be handled by the client code.
      * @return a string host_0:port,host_1:port
      */
    def connectionProps: String =
      if (nodeNames.isEmpty) throw new IllegalArgumentException("there is no nodes!!!")
      else nodeNames.map(n => s"$n:$clientPort").mkString(",")

    def clientPort: Int                        = settings.clientPort
    def zookeeperClusterKey: ObjectKey         = settings.zookeeperClusterKey
    def logDirs: String                        = settings.dataFolders
    def numberOfPartitions: Int                = settings.numberOfPartitions
    def numberOfReplications4OffsetsTopic: Int = settings.numberOfReplications4OffsetsTopic
    def numberOfNetworkThreads: Int            = settings.numberOfNetworkThreads
    def numberOfIoThreads: Int                 = settings.numberOfIoThreads
    def jvmPerformanceOptions: Option[String]  = settings.jvmPerformanceOptions
    def maxOfPoolMemory: Long                  = settings.maxOfPoolMemory
    def maxOfRequestMemory: Long               = settings.maxOfRequestMemory

    override def raw: Map[String, JsValue] = BROKER_CLUSTER_INFO_FORMAT.write(this).asJsObject.fields

    override def volumeMaps: Map[ObjectKey, String] = settings.volumeMaps
  }

  /**
    * exposed to configurator
    */
  private[ohara] implicit val BROKER_CLUSTER_INFO_FORMAT: RootJsonFormat[BrokerClusterInfo] =
    JsonRefiner(new RootJsonFormat[BrokerClusterInfo] {
      private[this] val format                            = jsonFormat5(BrokerClusterInfo)
      override def read(json: JsValue): BrokerClusterInfo = format.read(extractSetting(json.asJsObject))
      override def write(obj: BrokerClusterInfo): JsValue = flattenSettings(format.write(obj).asJsObject)
    })

  /**
    * used to generate the payload and url for POST/PUT request.
    * this request is extended by collie also so it is public than sealed.
    */
  trait Request extends ClusterRequest {
    def zookeeperClusterKey(zookeeperClusterKey: ObjectKey): Request.this.type =
      setting(ZOOKEEPER_CLUSTER_KEY_KEY, OBJECT_KEY_FORMAT.write(Objects.requireNonNull(zookeeperClusterKey)))

    @Optional("the default port is random")
    def clientPort(clientPort: Int): Request.this.type =
      setting(CLIENT_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(clientPort)))

    @Optional("the default port is random")
    def jmxPort(jmxPort: Int): Request.this.type =
      setting(JMX_PORT_KEY, JsNumber(CommonUtils.requireConnectionPort(jmxPort)))

    @Optional("default value is empty array")
    def tags(tags: Map[String, JsValue]): Request.this.type = setting(TAGS_KEY, JsObject(tags))

    @Optional("default is no volume mounted on data folder so all data is in container")
    def logDirs(volumeKeys: Set[ObjectKey]): Request.this.type = volumes(LOG_DIRS_KEY, volumeKeys)

    @Optional("default value is -1")
    def maxOfPoolMemory(sizeInBytes: Long): Request.this.type =
      setting(MAX_OF_POOL_MEMORY_BYTES, JsNumber(CommonUtils.requirePositiveLong(sizeInBytes)))

    @Optional("default value is 100 * 1024 * 1024")
    def maxOfRequestMemory(sizeInBytes: Long): Request.this.type =
      setting(MAX_OF_REQUEST_MEMORY_BYTES, JsNumber(CommonUtils.requirePositiveLong(sizeInBytes)))

    @Optional("default value is empty")
    def jvmPerformanceOptions(options: String): Request.this.type =
      setting(JVM_PERFORMANCE_OPTIONS_KEY, JsString(CommonUtils.requireNonEmpty(options)))

    /**
      * broker information creation.
      *  Here we open the access for reusing the creation to other module
      *
      * @return the payload of creation
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
    */
  sealed trait ExecutableRequest extends Request {
    def create()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo]
    def update()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo]
  }

  final class Access private[BrokerApi] extends ClusterAccess[Creation, Updating, BrokerClusterInfo](PREFIX) {
    override def query: Query[BrokerClusterInfo] = new Query[BrokerClusterInfo] {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[BrokerClusterInfo]] = list(request)
    }

    def request: ExecutableRequest = new ExecutableRequest {
      override def create()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo] = post(creation)

      override def update()(implicit executionContext: ExecutionContext): Future[BrokerClusterInfo] = put(key, updating)
    }
  }

  def access: Access = new Access
}
