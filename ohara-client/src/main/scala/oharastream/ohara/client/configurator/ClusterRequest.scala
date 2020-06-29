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
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsNumber, JsObject, JsString, JsValue}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
  * This is the basic cluster api request object.
  * A minimum request must set the nodeName.
  */
trait ClusterRequest {
  /**
    * a helper method to sub class to extract the key from settings
    * @return key from settings
    */
  protected def key: ObjectKey = ObjectKey.of(
    settings.get(GROUP_KEY).map(_.convertTo[String]).getOrElse(GROUP_DEFAULT),
    settings(NAME_KEY).convertTo[String]
  )

  protected val settings: mutable.Map[String, JsValue] = mutable.Map()

  @Optional("default key is a random string. But it is required in updating")
  def key(key: ObjectKey): ClusterRequest.this.type = {
    setting(NAME_KEY, JsString(key.name()))
    setting(GROUP_KEY, JsString(key.group()))
  }

  @Optional("default name is a random string. But it is required in updating")
  def name(name: String): ClusterRequest.this.type =
    setting(NAME_KEY, JsString(CommonUtils.requireNonEmpty(name)))

  @Optional("default is GROUP_DEFAULT")
  def group(group: String): ClusterRequest.this.type =
    setting(GROUP_KEY, JsString(CommonUtils.requireNonEmpty(group)))

  def nodeName(nodeName: String): ClusterRequest.this.type = nodeNames(Set(CommonUtils.requireNonEmpty(nodeName)))
  def nodeNames(nodeNames: Set[String]): ClusterRequest.this.type =
    setting(NODE_NAMES_KEY, JsArray(CommonUtils.requireNonEmpty(nodeNames.asJava).asScala.map(JsString(_)).toVector))

  @Optional("default value is empty array")
  def routes(routes: Map[String, String]): ClusterRequest.this.type =
    setting(ROUTES_KEY, JsObject(routes.map {
      case (k, v) => k -> JsString(v)
    }))

  @Optional("default value is 1024")
  def initHeap(sizeInMB: Int): ClusterRequest.this.type =
    setting(INIT_HEAP_KEY, JsNumber(CommonUtils.requirePositiveInt(sizeInMB)))

  @Optional("default value is 1024")
  def maxHeap(sizeInMB: Int): ClusterRequest.this.type =
    setting(MAX_HEAP_KEY, JsNumber(CommonUtils.requirePositiveInt(sizeInMB)))

  protected def volume(key: String, volumeKey: ObjectKey): ClusterRequest.this.type =
    setting(key, OBJECT_KEY_FORMAT.write(volumeKey))

  protected def volumes(key: String, volumeKeys: Set[ObjectKey]): ClusterRequest.this.type =
    setting(key, JsArray(volumeKeys.map(OBJECT_KEY_FORMAT.write).toVector))

  @Optional("extra settings is empty by default")
  def setting(key: String, value: JsValue): ClusterRequest.this.type =
    settings(Map(key -> value))

  @Optional("extra settings is empty by default")
  def settings(settings: Map[String, JsValue]): ClusterRequest.this.type = {
    // We don't have to check the settings is empty here for the following reasons:
    // 1) we may want to use the benefit of default creation without specify settings
    // 2) actual checking will be done in the json parser phase of creation or update
    this.settings ++= settings
    this
  }
}
