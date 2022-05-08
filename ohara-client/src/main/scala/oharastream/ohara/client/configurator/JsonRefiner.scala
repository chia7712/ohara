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

import spray.json.{JsValue, RootJsonFormat}

/**
  * Except for akka json function, ohara format expose the function used to verify the input key and value. The function
  * is useful in testing single item. For example, the information, which is carried by url, to restful APIs can be verified
  * by the check method.
  *
  * The exposed check make us have consistent behavior in parsing string to scala object. For example, the name is placed
  * at both url and payload, and both of them must go through the same name string check.
  * @tparam T object
  */
trait JsonRefiner[T] extends RootJsonFormat[T] {
  /**
    * create another refiner builder based on this and more rules.
    * @return new format
    */
  def toBuilder: JsonRefinerBuilder[T]

  /**
    * serialize and deseriailze the object. The input may be NOT same to output since the format rules may change something
    * according to the definitions
    * @param obj origin object
    * @return defined object
    */
  def refine(obj: T): T = read(write(obj))

  /**
    * verify the input key and value. It always pass if the input key is not associated to any check rule.
    * @param key input key
    * @param value input value
    */
  def check[Value <: JsValue](key: String, value: Value): Value = check(Map(key -> value))(key).asInstanceOf[Value]

  /**
    * verify the input keys and values. It always pass if the input keys are not associated to any check rule.
    * @param fields keys and values
    */
  def check(fields: Map[String, JsValue]): Map[String, JsValue]
}

object JsonRefiner {
  /**
    * create a builder to construct json refiner
    * @tparam T scala object to carry json data
    * @return builder
    */
  def builder[T]: JsonRefinerBuilder[T] = JsonRefinerBuilder[T]

  /**
    * wrap spray format to ohara format. It includes default rules from ohara format.
    * 1) reject empty string
    * 2) remove null field
    * @param format spray format
    * @tparam T data type
    * @return ohara format
    */
  def apply[T](format: RootJsonFormat[T]): JsonRefiner[T] = JsonRefiner.builder[T].format(format).build()
}
