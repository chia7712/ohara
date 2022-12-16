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

import java.io.File
import java.util.Objects
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, MediaTypes, Multipart, RequestEntity}
import oharastream.ohara.client.configurator.FileInfoApi.{ClassInfo, FIELD_NAME, FileInfo}
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.setting.{ClassType, ObjectKey, SettingDef, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsValue, RootJsonFormat}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

object InspectApi {
  val KIND: String                    = "inspect"
  val RDB_PREFIX: String              = "rdb"
  val TOPIC_TIMEOUT_KEY: String       = "timeout"
  val TOPIC_TIMEOUT_DEFAULT: Duration = Duration(3, TimeUnit.SECONDS)
  val TOPIC_LIMIT_KEY: String         = "limit"
  val TOPIC_LIMIT_DEFAULT: Int        = 5

  //-------------[rdb]-------------//
  /**
    * this key points to the settings passed by user. Those settings are converted to a "single" string and then it is
    * submitted to kafka to run the Validator. Kafka doesn't support various type of json so we have to transfer data
    * via pure string.
    */
  val SETTINGS_KEY = "settings"
  val TARGET_KEY   = "target"
  // TODO: We should use a temporary topic instead of fixed topic...by chia
  val INTERNAL_TOPIC_KEY: TopicKey = TopicKey.of(GROUP_DEFAULT, "_Validator_topic")

  /**
    * add this to setting and then the key pushed to topic will be same with the value
    */
  val REQUEST_ID = "requestId"

  //-------------[FILE]-------------//
  final case class ConfiguratorVersion(version: String, branch: String, user: String, revision: String, date: String)
  implicit val CONFIGURATOR_VERSION_FORMAT: RootJsonFormat[ConfiguratorVersion] = jsonFormat5(ConfiguratorVersion)

  final case class K8sUrls(coordinatorUrl: String, metricsUrl: Option[String])
  implicit val K8S_URL_FORMAT: RootJsonFormat[K8sUrls] = jsonFormat2(K8sUrls)
  final case class ConfiguratorInfo(versionInfo: ConfiguratorVersion, mode: String, k8sUrls: Option[K8sUrls])

  implicit val CONFIGURATOR_INFO_FORMAT: RootJsonFormat[ConfiguratorInfo] = jsonFormat3(ConfiguratorInfo)

  case class ServiceDefinition(imageName: String, settingDefinitions: Seq[SettingDef], classInfos: Seq[ClassInfo])

  implicit val SERVICE_DEFINITION_FORMAT: RootJsonFormat[ServiceDefinition] = jsonFormat3(ServiceDefinition)

  case class FileContent(classInfos: Seq[ClassInfo]) {
    def sourceClassInfos: Seq[ClassInfo]      = classInfos.filter(_.classType == ClassType.SOURCE)
    def sinkClassInfos: Seq[ClassInfo]        = classInfos.filter(_.classType == ClassType.SINK)
    def streamClassInfos: Seq[ClassInfo]      = classInfos.filter(_.classType == ClassType.STREAM)
    def partitionerClassInfos: Seq[ClassInfo] = classInfos.filter(_.classType == ClassType.PARTITIONER)
  }

  object FileContent {
    val empty: FileContent = FileContent(Seq.empty)
  }

  implicit val FILE_CONTENT_FORMAT: RootJsonFormat[FileContent] = new RootJsonFormat[FileContent] {
    private[this] val format = jsonFormat1(FileContent.apply)
    override def write(obj: FileContent): JsValue = JsObject {
      val fields = format.write(obj).asJsObject.fields
      // yep, the APIs compatibility
      fields + ("classes" -> fields("classInfos"))
    }

    override def read(json: JsValue): FileContent = format.read(json)
  }

  final case class RdbColumn(name: String, dataType: String, pk: Boolean)
  implicit val RDB_COLUMN_FORMAT: RootJsonFormat[RdbColumn] = jsonFormat3(RdbColumn)
  final case class RdbTable(
    catalogPattern: Option[String],
    schemaPattern: Option[String],
    name: String,
    columns: Seq[RdbColumn]
  )
  implicit val RDB_TABLE_FORMAT: RootJsonFormat[RdbTable] = jsonFormat4(RdbTable)

  final case class RdbQuery(
    url: String,
    user: String,
    workerClusterKey: ObjectKey,
    password: String,
    catalogPattern: Option[String],
    schemaPattern: Option[String],
    tableName: Option[String]
  )
  implicit val RDB_QUERY_FORMAT: JsonRefiner[RdbQuery] =
    JsonRefiner.builder[RdbQuery].format(jsonFormat7(RdbQuery)).build

  final case class RdbInfo(name: String, tables: Seq[RdbTable])
  implicit val RDB_INFO_FORMAT: RootJsonFormat[RdbInfo] = jsonFormat2(RdbInfo)

  final case class Message(
    partition: Int,
    offset: Long,
    timestamp: Long,
    sourceClass: Option[String],
    sourceKey: Option[ObjectKey],
    value: Option[JsValue],
    error: Option[String]
  )
  implicit val MESSAGE_FORMAT: RootJsonFormat[Message] = jsonFormat7(Message)

  final case class TopicData(messages: Seq[Message])
  implicit val TOPIC_DATA_FORMAT: RootJsonFormat[TopicData] = jsonFormat1(TopicData)

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  trait RdbRequest {
    def jdbcUrl(url: String): RdbRequest

    @Optional("server will match a broker cluster for you if the wk name is ignored")
    def workerClusterKey(workerClusterKey: ObjectKey): RdbRequest

    def user(user: String): RdbRequest

    def password(password: String): RdbRequest

    @Optional("default is null")
    def catalogPattern(catalogPattern: String): RdbRequest

    @Optional("default is null")
    def schemaPattern(schemaPattern: String): RdbRequest

    @Optional("default is null")
    def tableName(tableName: String): RdbRequest

    @VisibleForTesting
    private[configurator] def query: RdbQuery

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def query()(implicit executionContext: ExecutionContext): Future[RdbInfo]
  }

  trait TopicRequest {
    def key(key: TopicKey): TopicRequest
    def limit(limit: Int): TopicRequest
    def timeout(timeout: Duration): TopicRequest
    def query()(implicit executionContext: ExecutionContext): Future[TopicData]
  }

  trait FileRequest {
    def file(file: File): FileRequest
    def query()(implicit executionContext: ExecutionContext): Future[FileInfo]
  }

  final class Access extends BasicAccess(KIND) {
    def configuratorInfo()(implicit executionContext: ExecutionContext): Future[ConfiguratorInfo] =
      exec.get[ConfiguratorInfo, ErrorApi.Error](s"$url/$CONFIGURATOR_KIND")

    def zookeeperInfo()(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](s"$url/${ZookeeperApi.PREFIX}")

    def zookeeperInfo(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](urlBuilder.prefix(ZookeeperApi.PREFIX).key(key).build())

    def brokerInfo()(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](s"$url/${BrokerApi.PREFIX}")

    def brokerInfo(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](urlBuilder.prefix(BrokerApi.PREFIX).key(key).build())

    def workerInfo()(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](s"$url/${WorkerApi.PREFIX}")

    def workerInfo(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](urlBuilder.prefix(WorkerApi.PREFIX).key(key).build())

    def streamInfo()(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](s"$url/${StreamApi.PREFIX}")

    def streamInfo(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](urlBuilder.prefix(StreamApi.PREFIX).key(key).build())

    def shabondiInfo()(implicit executionContext: ExecutionContext): Future[ServiceDefinition] =
      exec.get[ServiceDefinition, ErrorApi.Error](s"$url/${ShabondiApi.PREFIX}")

    def rdbRequest: RdbRequest = new RdbRequest {
      private[this] var jdbcUrl: String             = _
      private[this] var user: String                = _
      private[this] var password: String            = _
      private[this] var workerClusterKey: ObjectKey = _
      private[this] var catalogPattern: String      = _
      private[this] var schemaPattern: String       = _
      private[this] var tableName: String           = _

      override def jdbcUrl(jdbcUrl: String): RdbRequest = {
        this.jdbcUrl = CommonUtils.requireNonEmpty(jdbcUrl)
        this
      }

      override def workerClusterKey(workerClusterKey: ObjectKey): RdbRequest = {
        this.workerClusterKey = Objects.requireNonNull(workerClusterKey)
        this
      }

      override def user(user: String): RdbRequest = {
        this.user = CommonUtils.requireNonEmpty(user)
        this
      }

      override def password(password: String): RdbRequest = {
        this.password = CommonUtils.requireNonEmpty(password)
        this
      }

      override def catalogPattern(catalogPattern: String): RdbRequest = {
        this.catalogPattern = CommonUtils.requireNonEmpty(catalogPattern)
        this
      }

      override def schemaPattern(schemaPattern: String): RdbRequest = {
        this.schemaPattern = CommonUtils.requireNonEmpty(schemaPattern)
        this
      }

      override def tableName(tableName: String): RdbRequest = {
        this.tableName = CommonUtils.requireNonEmpty(tableName)
        this
      }

      override private[configurator] def query: RdbQuery = RdbQuery(
        url = CommonUtils.requireNonEmpty(jdbcUrl),
        user = CommonUtils.requireNonEmpty(user),
        password = CommonUtils.requireNonEmpty(password),
        workerClusterKey = Objects.requireNonNull(workerClusterKey),
        catalogPattern = Option(catalogPattern).map(CommonUtils.requireNonEmpty),
        schemaPattern = Option(schemaPattern).map(CommonUtils.requireNonEmpty),
        tableName = Option(tableName).map(CommonUtils.requireNonEmpty)
      )

      override def query()(implicit executionContext: ExecutionContext): Future[RdbInfo] =
        exec.post[RdbQuery, RdbInfo, ErrorApi.Error](s"$url/$RDB_PREFIX", query)
    }

    def topicRequest: TopicRequest = new TopicRequest {
      private[this] var key: TopicKey = _
      private[this] var limit         = TOPIC_LIMIT_DEFAULT
      private[this] var timeout       = TOPIC_TIMEOUT_DEFAULT

      override def key(key: TopicKey): TopicRequest = {
        this.key = Objects.requireNonNull(key)
        this
      }

      override def limit(limit: Int): TopicRequest = {
        this.limit = CommonUtils.requirePositiveInt(limit)
        this
      }

      override def timeout(timeout: Duration): TopicRequest = {
        this.timeout = Objects.requireNonNull(timeout)
        this
      }

      override def query()(implicit executionContext: ExecutionContext): Future[TopicData] =
        exec.post[TopicData, ErrorApi.Error](
          urlBuilder
            .key(key)
            .prefix(TopicApi.PREFIX)
            .param(TOPIC_LIMIT_KEY, limit.toString)
            .param(TOPIC_TIMEOUT_KEY, timeout.toMillis.toString)
            .build()
        )
    }

    def fileRequest: FileRequest = new FileRequest {
      private[this] var file: File = _

      override def file(file: File): FileRequest = {
        this.file = Objects.requireNonNull(file)
        this
      }

      override def query()(implicit executionContext: ExecutionContext): Future[FileInfo] =
        Marshal(
          Multipart.FormData(
            // add file
            Multipart.FormData.BodyPart(
              FIELD_NAME,
              HttpEntity.fromFile(MediaTypes.`application/octet-stream`, file),
              Map("filename" -> file.getName)
            )
          )
        ).to[RequestEntity]
          .map(e => HttpRequest(HttpMethods.POST, uri = s"$url/${FileInfoApi.PREFIX}", entity = e))
          .flatMap(exec.request[FileInfo, ErrorApi.Error])
    }
  }

  def access: Access = new Access
}
