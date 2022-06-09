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
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.SettingDef.{Reference, Type}
import oharastream.ohara.common.setting.{ObjectKey, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka
import oharastream.ohara.kafka.{PartitionInfo, PartitionNode}
import org.apache.kafka.common.config.TopicConfig
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

object TopicApi {
  val KIND: String                          = SettingDef.Reference.TOPIC.name().toLowerCase
  val PREFIX: String                        = "topics"
  val BROKER_CLUSTER_KEY_KEY                = "brokerClusterKey"
  val NUMBER_OF_PARTITIONS_KEY              = "numberOfPartitions"
  val NUMBER_OF_REPLICATIONS_KEY            = "numberOfReplications"
  val SEGMENT_BYTES_KEY: String             = TopicConfig.SEGMENT_BYTES_CONFIG
  val SEGMENT_TIME_KEY: String              = "segment.time"
  val CLEANUP_POLICY_KEY: String            = TopicConfig.CLEANUP_POLICY_CONFIG
  val MIN_CLEANABLE_DIRTY_RATIO_KEY: String = TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG
  val RETENTION_TIME_KEY: String            = "retention.time"
  val COMPRESSION_TYPE_CONFIG: String = TopicConfig.COMPRESSION_TYPE_CONFIG

  /**
    * Use to convert ohara key to kafka key. Not all kafka namings are suitable to ohara. For example, the postfix "ms"
    * is not a good design to ohara since ohara supports Duration.
    */
  private[this] val TO_KAFKA_CONFIG_KEY: Map[String, String] = Map(
    RETENTION_TIME_KEY -> TopicConfig.RETENTION_MS_CONFIG,
    SEGMENT_TIME_KEY   -> TopicConfig.SEGMENT_MS_CONFIG
  )

  /**
    * the config with this group is mapped to kafka's custom config. Kafka divide configs into two parts.
    * 1) required configs (number of partitions and number of replications)
    * 2) custom configs (those config must be able to convert to string)
    *
    * Furthermore, kafka forbids us to put required configs to custom configs. Hence, we have to mark the custom config
    * in order to filter the custom from settings (see Creation).
    */
  private[this] val CONFIGS_GROUP = "configs"

  sealed abstract class CleanupPolicy
  object CleanupPolicy extends Enum[CleanupPolicy] {
    case object DELETE extends CleanupPolicy {
      override def toString: String = TopicConfig.CLEANUP_POLICY_DELETE
    }

    case object COMPACT extends CleanupPolicy {
      override def toString: String = TopicConfig.CLEANUP_POLICY_COMPACT
    }
  }

  implicit val CLEANUP_POLICY_FORMAT: RootJsonFormat[CleanupPolicy] = new RootJsonFormat[CleanupPolicy] {
    override def read(json: JsValue): CleanupPolicy = CleanupPolicy.forName(json.convertTo[String])
    override def write(obj: CleanupPolicy): JsValue = JsString(obj.toString)
  }

  val DEFINITIONS: Seq[SettingDef] = DefinitionCollector()
    .addFollowupTo("core")
    .group()
    .name()
    .tags()
    .definition(
      _.key(BROKER_CLUSTER_KEY_KEY)
        .documentation("broker cluster used to store data for this worker cluster")
        .required(Type.OBJECT_KEY)
        .reference(Reference.BROKER)
    )
    .addFollowupTo("performance")
    .definition(
      _.key(NUMBER_OF_PARTITIONS_KEY)
        .documentation("the number of partitions")
        .positiveNumber(1)
    )
    .definition(
      _.key(NUMBER_OF_REPLICATIONS_KEY)
        .documentation("the number of replications")
        .positiveNumber(1.asInstanceOf[Short])
    )
    .addFollowupTo(CONFIGS_GROUP)
    .definition(
      _.key(SEGMENT_BYTES_KEY)
        .documentation(TopicConfig.SEGMENT_BYTES_DOC)
        .positiveNumber(1L * 1024L * 1024L * 1024L)
    )
    .definition(
      _.key(SEGMENT_TIME_KEY)
        .documentation(TopicConfig.SEGMENT_MS_DOC)
        .optional(java.time.Duration.ofDays(7))
    )
    .definition(
      _.key(CLEANUP_POLICY_KEY)
        .documentation(TopicConfig.CLEANUP_POLICY_DOC)
        .optional(TopicConfig.CLEANUP_POLICY_DELETE)
    )
    .definition(
      _.key(MIN_CLEANABLE_DIRTY_RATIO_KEY)
        .documentation(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC)
        .optional(0.5d)
    )
    .definition(
      _.key(RETENTION_TIME_KEY)
        .documentation(TopicConfig.RETENTION_MS_DOC)
        .optional(java.time.Duration.ofDays(7))
    )
    .definition(
      _.key(COMPRESSION_TYPE_CONFIG)
        .documentation(TopicConfig.COMPRESSION_TYPE_DOC)
        .optional("producer")
    )
    .result

  final class Updating private[TopicApi] (val raw: Map[String, JsValue]) extends BasicUpdating {
    def brokerClusterKey: Option[ObjectKey] = raw.get(BROKER_CLUSTER_KEY_KEY).map(_.convertTo[ObjectKey])
    private[TopicApi] def numberOfPartitions: Option[Int] =
      raw.get(NUMBER_OF_PARTITIONS_KEY).map(_.convertTo[Int])

    private[TopicApi] def numberOfReplications: Option[Short] =
      raw.get(NUMBER_OF_REPLICATIONS_KEY).map(_.convertTo[Short])

    private[TopicApi] def group: Option[String] = raw.get(GROUP_KEY).map(_.convertTo[String])

    private[TopicApi] def name: Option[String] = raw.get(NAME_KEY).map(_.convertTo[String])

    def cleanupPolicy: Option[CleanupPolicy] = raw.get(CLEANUP_POLICY_KEY).map(_.convertTo[CleanupPolicy])

    def minCleanableDirtyRatio: Option[Double] = raw.get(MIN_CLEANABLE_DIRTY_RATIO_KEY).map(_.convertTo[Double])

    def retention: Option[Duration] = raw.get(RETENTION_TIME_KEY).map(_.convertTo[Duration])
  }

  implicit val UPDATING_FORMAT: RootJsonFormat[Updating] =
    JsonRefiner
      .builder[Updating]
      .format(new RootJsonFormat[Updating] {
        override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)
        override def write(obj: Updating): JsValue = JsObject(obj.raw)
      })
      .ignoreKeys(RUNTIME_KEYS)
      .build()

  final class Creation private[TopicApi] (val raw: Map[String, JsValue])
      extends oharastream.ohara.client.configurator.BasicCreation {
    private[this] implicit def update(raw: Map[String, JsValue]): Updating = new Updating(raw)

    override def key: TopicKey = TopicKey.of(group, name)

    def brokerClusterKey: ObjectKey = raw.brokerClusterKey.get

    def numberOfPartitions: Int     = raw.numberOfPartitions.get
    def numberOfReplications: Short = raw.numberOfReplications.get

    override def group: String = raw.group.get

    override def name: String = raw.name.get

    override def tags: Map[String, JsValue] = raw.tags.get

    def cleanupPolicy: CleanupPolicy = raw.cleanupPolicy.get

    def minCleanableDirtyRatio: Double = raw.minCleanableDirtyRatio.get

    def retention: Duration = raw.retention.get
  }

  implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    // this object is open to user define the (group, name) in UI, we need to handle the key rules
    limitsOfKey[Creation]
      .format(new RootJsonFormat[Creation] {
        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
        override def write(obj: Creation): JsValue = JsObject(obj.raw)
      })
      // TODO: topic definitions may be changed by different Broker images so this check is dangerous
      .definitions(DEFINITIONS)
      .ignoreKeys(RUNTIME_KEYS)
      .build

  import MetricsApi._

  abstract sealed class TopicState(val name: String) extends Serializable
  object TopicState extends Enum[TopicState] {
    case object RUNNING extends TopicState("RUNNING")
  }

  implicit val TOPIC_STATE_FORMAT: RootJsonFormat[TopicState] = new RootJsonFormat[TopicState] {
    override def read(json: JsValue): TopicState = TopicState.forName(json.convertTo[String].toUpperCase)
    override def write(obj: TopicState): JsValue = JsString(obj.name)
  }

  implicit val PARTITION_NODE_FORMAT: RootJsonFormat[PartitionNode] = new RootJsonFormat[PartitionNode] {
    private[this] case class _Node(id: Int, host: String, port: Int)
    private[this] val format = jsonFormat3(_Node)
    override def read(json: JsValue): PartitionNode = {
      val node = format.read(json)
      new PartitionNode(node.id, node.host, node.port)
    }
    override def write(obj: PartitionNode): JsValue =
      format.write(
        _Node(
          id = obj.id,
          host = obj.host,
          port = obj.port
        )
      )
  }

  implicit val PARTITION_INFO_FORMAT: RootJsonFormat[PartitionInfo] = new RootJsonFormat[PartitionInfo] {
    private[this] case class _PartitionInfo(
      id: Int,
      leader: PartitionNode,
      replicas: Seq[PartitionNode],
      inSyncReplicas: Seq[PartitionNode],
      beginningOffset: Long,
      endOffset: Long
    )
    private[this] val format = jsonFormat6(_PartitionInfo)
    override def read(json: JsValue): kafka.PartitionInfo = {
      val partitionInfo = format.read(json)
      new kafka.PartitionInfo(
        partitionInfo.id,
        partitionInfo.leader,
        partitionInfo.replicas.asJava,
        partitionInfo.inSyncReplicas.asJava,
        partitionInfo.beginningOffset,
        partitionInfo.endOffset
      )
    }
    override def write(obj: kafka.PartitionInfo): JsValue =
      format.write(
        _PartitionInfo(
          id = obj.id,
          leader = obj.leader,
          replicas = obj.replicas.asScala.toSeq,
          inSyncReplicas = obj.inSyncReplicas.asScala.toSeq,
          beginningOffset = obj.beginningOffset,
          endOffset = obj.endOffset
        )
      )
  }

  case class TopicInfo(
    settings: Map[String, JsValue],
    partitionInfos: Seq[PartitionInfo],
    nodeMetrics: Map[String, Metrics],
    state: Option[TopicState],
    override val lastModified: Long
  ) extends Data
      with Metricsable {
    private[this] implicit def creation(settings: Map[String, JsValue]): Creation = new Creation(settings)

    override def key: TopicKey = TopicKey.of(group, name)
    override def kind: String  = KIND

    /**
      * kafka topic does not support to group topic so we salt the group with name.
      * @return topic name for kafka
      */
    def topicNameOnKafka: String = key.topicNameOnKafka

    def brokerClusterKey: ObjectKey = settings.brokerClusterKey
    def numberOfPartitions: Int     = settings.numberOfPartitions

    def numberOfReplications: Short = settings.numberOfReplications

    /**
      * @return the custom configs. the core configs are not included
      */
    def configs: Map[String, JsValue] = settings.filter {
      case (key, _) =>
        DEFINITIONS.filter(_.group() == CONFIGS_GROUP).exists(_.key() == key)
    }

    /**
      * @return custom configs in kafka format
      */
    def kafkaConfigs: Map[String, String] = configs.map {
      case (key, value) =>
        // convert the duration type to milliseconds
        val convertedValue = DEFINITIONS
          .find(_.key() == key)
          .filter(_.valueType() == SettingDef.Type.DURATION)
          .map(_ => JsNumber(DURATION_FORMAT.read(value).toMillis))
          .getOrElse(value)
        // convert ohara key to kafka key.
        TO_KAFKA_CONFIG_KEY.getOrElse(key, key) -> (convertedValue match {
          case JsString(v) => v
          case _           => convertedValue.toString()
        })
    }

    override def raw: Map[String, JsValue] = TOPIC_INFO_FORMAT.write(this).asJsObject.fields

    def cleanupPolicy: CleanupPolicy = raw.cleanupPolicy

    def minCleanableDirtyRatio: Double = raw.minCleanableDirtyRatio

    def retention: Duration = raw.retention
  }

  implicit val TOPIC_INFO_FORMAT: RootJsonFormat[TopicInfo] = JsonRefiner(new RootJsonFormat[TopicInfo] {
    private[this] val format                    = jsonFormat5(TopicInfo)
    override def read(json: JsValue): TopicInfo = format.read(extractSetting(json.asJsObject))
    override def write(obj: TopicInfo): JsValue = flattenSettings(format.write(obj).asJsObject)
  })

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  sealed trait Request {
    private[this] val settings: mutable.Map[String, JsValue] = mutable.Map()

    /**
      * set the group and name via key
      * @param topicKey topic key
      * @return this request
      */
    def key(topicKey: TopicKey): Request = {
      group(topicKey.group())
      name(topicKey.name())
    }

    @Optional("default group is \"default\"")
    def group(group: String): Request = setting(GROUP_KEY, JsString(CommonUtils.requireNonEmpty(group)))

    @Optional("default name is a random string. But it is required in updating")
    def name(name: String): Request =
      setting(NAME_KEY, JsString(CommonUtils.requireNonEmpty(name)))

    @Optional("server will match a broker cluster for you if the bk name is ignored")
    def brokerClusterKey(brokerClusterKey: ObjectKey): Request =
      setting(BROKER_CLUSTER_KEY_KEY, OBJECT_KEY_FORMAT.write(brokerClusterKey))

    @Optional("default value is DEFAULT_NUMBER_OF_PARTITIONS")
    def numberOfPartitions(numberOfPartitions: Int): Request =
      setting(NUMBER_OF_PARTITIONS_KEY, JsNumber(CommonUtils.requirePositiveInt(numberOfPartitions)))

    @Optional("default value is DEFAULT_NUMBER_OF_REPLICATIONS")
    def numberOfReplications(numberOfReplications: Short): Request =
      setting(NUMBER_OF_REPLICATIONS_KEY, JsNumber(CommonUtils.requirePositiveShort(numberOfReplications)))

    @Optional("default value is empty array")
    def tags(tags: Map[String, JsValue]): Request =
      setting(TAGS_KEY, JsObject(Objects.requireNonNull(tags)))

    @Optional("default value is delete policy")
    def cleanupPolicy(policy: CleanupPolicy): Request = setting(CLEANUP_POLICY_KEY, CLEANUP_POLICY_FORMAT.write(policy))

    def minCleanableDirtyRatio(value: Double): Request = setting(MIN_CLEANABLE_DIRTY_RATIO_KEY, JsNumber(value))

    def retention(value: Duration): Request = setting(RETENTION_TIME_KEY, DURATION_FORMAT.write(value))

    def setting(key: String, value: JsValue): Request = settings(Map(key -> value))

    def settings(settings: Map[String, JsValue]): Request = {
      this.settings ++= settings
      this
    }

    /**
      * Creation instance includes many useful parsers for custom settings so we open it to code with a view to reusing
      * those convenient parsers.
      * @return the payload of creation
      */
    final def creation: Creation =
      // rewrite the creation via format since the format will auto-complete the creation
      // this make the creaion is consistent to creation sent to server
      CREATION_FORMAT.read(CREATION_FORMAT.write(new Creation(settings.toMap)))

    private[configurator] final def updating: Updating =
      // rewrite the update via format since the format will auto-complete the creation
      // this make the update is consistent to creation sent to server
      UPDATING_FORMAT.read(UPDATING_FORMAT.write(new Updating(settings.toMap)))

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[TopicInfo]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[TopicInfo]
  }

  sealed trait Query extends BasicQuery[TopicInfo] {
    import spray.json._
    def state(value: TopicState): Query = setting("state", value.name)

    def brokerClusterKey(key: ObjectKey): Query = setting(BROKER_CLUSTER_KEY_KEY, ObjectKey.toJsonString(key).parseJson)

    // TODO: there are a lot of settings which is worth of having parameters ... by chia
  }

  class Access private[configurator]
      extends oharastream.ohara.client.configurator.Access[Creation, Updating, TopicInfo](PREFIX) {
    def start(key: TopicKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, START_COMMAND)
    def stop(key: TopicKey)(implicit executionContext: ExecutionContext): Future[Unit]  = put(key, STOP_COMMAND)

    def query: Query = new Query {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[TopicInfo]] = list(request)
    }

    def request: Request = new Request {
      override def create()(implicit executionContext: ExecutionContext): Future[TopicInfo] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[TopicInfo] =
        put(TopicKey.of(updating.group.getOrElse(GROUP_DEFAULT), updating.name.get), updating)
    }
  }

  def access: Access = new Access
}
