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

import oharastream.ohara.client.configurator.QueryRequest
import oharastream.ohara.common.util.CommonUtils
import spray.json.{JsObject, JsString, JsValue}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
  * this basic query includes the parameters owned by all objects. Also, it host the query structure and it generate
  * the query request for the sub classes.
  * @tparam T result type
  */
trait BasicQuery[T] {
  private[this] val paras: mutable.Map[String, String] = mutable.Map[String, String]()

  def name(value: String): BasicQuery.this.type = setting("name", value)

  def group(value: String): BasicQuery.this.type = setting("group", value)

  def noState: BasicQuery.this.type = setting("state", "none")

  /**
    * complete match to the "tags"
    * @param tags
    * @return this query
    */
  def tags(tags: Map[String, JsValue]): BasicQuery.this.type = setting("tags", JsObject(tags).toString())

  def lastModified(value: Long): BasicQuery.this.type = setting("lastModified", value.toString)

  def setting(key: String, value: JsValue): BasicQuery.this.type =
    setting(key, value match {
      case JsString(s) => s
      case _           => value.toString
    })

  protected def setting(key: String, value: String): BasicQuery.this.type = {
    paras += (CommonUtils.requireNonEmpty(key) -> CommonUtils.requireNonEmpty(value))
    this
  }

  /**
    * send the LIST request with query parameters.
    * @param executionContext thread pool
    * @return results
    */
  final def execute()(implicit executionContext: ExecutionContext): Future[Seq[T]] =
    doExecute(QueryRequest(paras.toMap))

  protected def doExecute(request: QueryRequest)(implicit executionContext: ExecutionContext): Future[Seq[T]]
}
