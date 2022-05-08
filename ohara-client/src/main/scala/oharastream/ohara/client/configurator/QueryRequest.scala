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

/**
  * we introduces a interface to represent the query request as the pure map[String, String] may be too weak in the future.
  * Changing the input type always causes a bunch of code changes to code base so we wrap the map[String, String] via
  * a interface. It enables us to carry more data without changing input type.
  */
trait QueryRequest {
  def raw: Map[String, String]
}

object QueryRequest {
  def apply(_raw: Map[String, String]): QueryRequest = new QueryRequest {
    override def raw: Map[String, String] = _raw
  }
}
