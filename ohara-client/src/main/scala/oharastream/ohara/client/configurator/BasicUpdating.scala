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

import spray.json.JsValue

/**
  * this is a basic interface of request to update a normal object resource.
  * We separate this interface with basic Data since request payload does not mean to be a "store-able" data
  */
trait BasicUpdating {
  /**
    * @return user-defined fields
    */
  def tags: Option[Map[String, JsValue]] = raw.get(TAGS_KEY).map(_.asJsObject.fields)

  /**
    * @return the raw settings from request
    */
  def raw: Map[String, JsValue]
}
