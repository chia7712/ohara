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

import oharastream.ohara.common.setting.ObjectKey
import spray.json.JsValue

import spray.json.DefaultJsonProtocol._

/**
  * this is a basic interface of request to create a normal object resource.
  * We separate this interface with basic Data since request payload does not mean to be a "store-able" data
  */
trait BasicCreation {
  def key: ObjectKey = ObjectKey.of(group, name)

  /**
    * @return object group
    */
  def group: String = raw(GROUP_KEY).convertTo[String]

  /**
    * @return object name
    */
  def name: String = raw(NAME_KEY).convertTo[String]

  /**
    * @return user-defined fields
    */
  def tags: Map[String, JsValue] = raw(TAGS_KEY).asJsObject.fields

  /**
    * @return the raw settings from request
    */
  def raw: Map[String, JsValue]
}
