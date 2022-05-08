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

import oharastream.ohara.client.configurator.{Data, QueryRequest}
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsValue, RootJsonFormat}

import scala.concurrent.{ExecutionContext, Future}

object PipelineApi {
  val KIND: String   = SettingDef.Reference.PIPELINE.name().toLowerCase
  val PREFIX: String = "pipelines"

  /**
    * action key. it is used to auto-remove the existent objs from endpoints,
    */
  val REFRESH_COMMAND: String = "refresh"

  final case class Endpoint(group: String, name: String, kind: String) {
    def key: ObjectKey = ObjectKey.of(group, name)
  }
  implicit val ENDPOINT_FORMAT: JsonRefiner[Endpoint] =
    JsonRefiner
      .builder[Endpoint]
      .format(jsonFormat3(Endpoint))
      .nullToString(GROUP_KEY, GROUP_DEFAULT)
      .build

  final case class Updating(
    endpoints: Option[Set[Endpoint]],
    override val tags: Option[Map[String, JsValue]]
  ) extends BasicUpdating {
    override def raw: Map[String, JsValue] = UPDATING_FORMAT.write(this).asJsObject.fields
  }

  implicit val UPDATING_FORMAT: RootJsonFormat[Updating] =
    JsonRefiner.builder[Updating].format(jsonFormat2(Updating)).build

  final case class Creation(
    override val group: String,
    override val name: String,
    endpoints: Set[Endpoint],
    override val tags: Map[String, JsValue]
  ) extends oharastream.ohara.client.configurator.BasicCreation {
    override def raw: Map[String, JsValue] = CREATION_FORMAT.write(this).asJsObject.fields
  }

  implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    // this object is open to user define the (group, name) in UI, we need to handle the key rules
    rulesOfKey[Creation]
      .format(jsonFormat4(Creation))
      .nullToEmptyObject(TAGS_KEY)
      .nullToEmptyArray("endpoints")
      .build

  import MetricsApi._

  /**
    * Metricsable offers many helper methods so we let ObjectAbstract extend it.
    */
  final case class ObjectAbstract(
    group: String,
    name: String,
    kind: String,
    className: Option[String],
    state: Option[String],
    error: Option[String],
    nodeMetrics: Map[String, Metrics],
    lastModified: Long,
    tags: Map[String, JsValue]
  ) extends Metricsable {
    def key: ObjectKey = ObjectKey.of(group, name)
  }

  implicit val OBJECT_ABSTRACT_FORMAT: RootJsonFormat[ObjectAbstract] = new RootJsonFormat[ObjectAbstract] {
    private[this] val format                         = jsonFormat9(ObjectAbstract)
    override def read(json: JsValue): ObjectAbstract = format.read(json)
    override def write(obj: ObjectAbstract): JsValue = format.write(obj)
  }

  final case class Pipeline(
    override val group: String,
    override val name: String,
    endpoints: Set[Endpoint],
    objects: Set[ObjectAbstract],
    jarKeys: Set[ObjectKey],
    override val lastModified: Long,
    override val tags: Map[String, JsValue]
  ) extends Data {
    override def kind: String = KIND

    override def raw: Map[String, JsValue] = PIPELINE_FORMAT.write(this).asJsObject.fields
  }

  implicit val PIPELINE_FORMAT: RootJsonFormat[Pipeline] = jsonFormat7(Pipeline)

  /**
    * used to generate the payload and url for POST/PUT request.
    */
  trait Request {
    /**
      * set the group and name via key
      * @param objectKey object key
      * @return this request
      */
    def key(objectKey: ObjectKey): Request = {
      group(objectKey.group())
      name(objectKey.name())
    }

    @Optional("default def is a GROUP_DEFAULT")
    def group(group: String): Request

    @Optional("default name is a random string. But it is required in updating")
    def name(name: String): Request

    @Optional("default value is empty")
    def endpoint(data: Data): Request = endpoints(Seq(data))

    @Optional("default value is empty")
    def endpoints(data: Seq[Data]): Request
    @Optional("default value is empty array in creation and None in update")
    def tags(tags: Map[String, JsValue]): Request

    private[configurator] def creation: Creation

    private[configurator] def updating: Updating

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[Pipeline]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[Pipeline]
  }

  sealed trait Query extends BasicQuery[Pipeline] {
    // TODO: there are a lot of settings which is worth of having parameters ... by chia
  }

  class Access private[configurator]
      extends oharastream.ohara.client.configurator.Access[Creation, Updating, Pipeline](PREFIX) {
    def refresh(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[Unit] = put(key, REFRESH_COMMAND)

    def query: Query = new Query {
      override protected def doExecute(request: QueryRequest)(
        implicit executionContext: ExecutionContext
      ): Future[Seq[Pipeline]] = list(request)
    }

    def request: Request = new Request {
      private[this] var group: String = GROUP_DEFAULT
      private[this] var name: String  = _
      // TODO: remove this helper (https://github.com/oharastream/ohara/issues/3530)
      private[this] var endpoints: Set[Endpoint]   = _
      private[this] var tags: Map[String, JsValue] = _

      override def group(group: String): Request = {
        this.group = CommonUtils.requireNonEmpty(group)
        this
      }

      override def name(name: String): Request = {
        this.name = CommonUtils.requireNonEmpty(name)
        this
      }

      override def endpoints(data: Seq[Data]): Request = {
        val newEndpoints = data.map(d => Endpoint(group = d.group, name = d.name, kind = d.kind)).toSet
        if (this.endpoints == null) this.endpoints = newEndpoints
        else this.endpoints ++= newEndpoints
        this
      }

      override def tags(tags: Map[String, JsValue]): Request = {
        if (this.tags == null) this.tags = Objects.requireNonNull(tags)
        else this.tags ++= Objects.requireNonNull(tags)
        this
      }

      override private[configurator] def creation: Creation =
        // auto-complete the creation via our refiner
        CREATION_FORMAT.read(
          CREATION_FORMAT.write(
            Creation(
              group = CommonUtils.requireNonEmpty(group),
              name = if (CommonUtils.isEmpty(name)) CommonUtils.randomString(10) else name,
              endpoints = if (endpoints == null) Set.empty else endpoints,
              tags = if (tags == null) Map.empty else tags
            )
          )
        )

      override private[configurator] def updating: Updating =
        // auto-complete the updating via our refiner
        UPDATING_FORMAT.read(
          UPDATING_FORMAT.write(
            Updating(
              endpoints = Option(endpoints),
              tags = Option(tags)
            )
          )
        )

      override def create()(implicit executionContext: ExecutionContext): Future[Pipeline] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[Pipeline] =
        put(ObjectKey.of(group, name), updating)
    }
  }

  def access: Access = new Access
}
