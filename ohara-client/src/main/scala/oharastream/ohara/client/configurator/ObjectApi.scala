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

import oharastream.ohara.common.setting.{ObjectKey, SettingDef}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object ObjectApi {
  val KIND: String   = SettingDef.Reference.OBJECT.name().toLowerCase
  val PREFIX: String = "objects"

  final class Creation(val raw: Map[String, JsValue]) extends BasicCreation {
    private[this] implicit def update(raw: Map[String, JsValue]): Updating = new Updating(raw)

    override def group: String = raw.group.get

    override def name: String = raw.name.get

    override def tags: Map[String, JsValue] = raw.tags.get
  }
  private[ohara] implicit val CREATION_FORMAT: JsonRefiner[Creation] =
    rulesOfKey[Creation]
      .format(new RootJsonFormat[Creation] {
        override def write(obj: Creation): JsValue = JsObject(obj.raw)

        override def read(json: JsValue): Creation = new Creation(json.asJsObject.fields)
      })
      .nullToEmptyObject(TAGS_KEY)
      .build

  final class Updating(val raw: Map[String, JsValue]) extends BasicUpdating {
    // We use the update parser to get the name and group
    private[ObjectApi] def name: Option[String]  = raw.get(NAME_KEY).map(_.convertTo[String])
    private[ObjectApi] def group: Option[String] = raw.get(GROUP_KEY).map(_.convertTo[String])
  }

  implicit val UPDATING_FORMAT: RootJsonFormat[Updating] = JsonRefiner(new RootJsonFormat[Updating] {
    override def read(json: JsValue): Updating = new Updating(json.asJsObject.fields)

    override def write(obj: Updating): JsValue = JsObject(obj.raw)
  })

  final class ObjectInfo private[configurator] (val settings: Map[String, JsValue]) extends Data with Serializable {
    override def kind: String = KIND

    override def equals(obj: Any): Boolean = obj match {
      case other: ObjectInfo => other.settings == settings
      case _                 => false
    }

    override def hashCode(): Int = settings.hashCode()

    override def toString: String = settings.toString()

    override def raw: Map[String, JsValue] = settings
  }

  object ObjectInfo {
    def apply(settings: Map[String, JsValue], lastModified: Long): ObjectInfo =
      new ObjectInfo(settings + (LAST_MODIFIED_KEY -> JsNumber(lastModified)))
  }

  implicit val OBJECT_FORMAT: RootJsonFormat[ObjectInfo] = JsonRefiner(new RootJsonFormat[ObjectInfo] {
    override def write(obj: ObjectInfo): JsValue = JsObject(obj.settings)
    override def read(json: JsValue): ObjectInfo = new ObjectInfo(json.asJsObject.fields)
  })

  trait Request {
    private[this] val settings: mutable.Map[String, JsValue] = mutable.Map[String, JsValue]()

    def name(name: String): Request =
      settings(Map(NAME_KEY -> JsString(name)))

    def group(group: String): Request =
      settings(Map(GROUP_KEY -> JsString(group)))

    def key(key: ObjectKey): Request =
      settings(Map(NAME_KEY -> JsString(key.name()), GROUP_KEY -> JsString(key.group())))

    def settings(settings: Map[String, JsValue]): Request = {
      this.settings ++= settings
      this
    }

    def creation: Creation =
      CREATION_FORMAT.read(CREATION_FORMAT.write(new Creation(settings.toMap)))

    def updating: Updating =
      UPDATING_FORMAT.read(UPDATING_FORMAT.write(new Updating(settings.toMap)))

    /**
      * generate the POST request
      * @param executionContext thread pool
      * @return created data
      */
    def create()(implicit executionContext: ExecutionContext): Future[ObjectInfo]

    /**
      * generate the PUT request
      * @param executionContext thread pool
      * @return updated/created data
      */
    def update()(implicit executionContext: ExecutionContext): Future[ObjectInfo]
  }

  class Access private[configurator]
      extends oharastream.ohara.client.configurator.Access[Creation, Updating, ObjectInfo](PREFIX) {
    def request: Request = new Request {
      override def create()(implicit executionContext: ExecutionContext): Future[ObjectInfo] = post(creation)
      override def update()(implicit executionContext: ExecutionContext): Future[ObjectInfo] =
        put(ObjectKey.of(updating.group.getOrElse(GROUP_DEFAULT), updating.name.get), updating)
    }
  }

  def access: Access = new Access
}
