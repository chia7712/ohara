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
import oharastream.ohara.common.setting.ObjectKey
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

/**
  * A general class used to access data of configurator. The protocol is based on http (restful APIs), and this implementation is built by akka
  * http. All data in ohara have same APIs so we extract this layer to make our life easily.
  * @param prefixPath path to data
  * @param rm formatter of response
  * @tparam Res type of Response
  */
abstract class Access[Creation <: BasicCreation, Updating, Res] private[configurator] (prefixPath: String)(
  implicit rm: RootJsonFormat[Creation],
  rm1: RootJsonFormat[Updating],
  rm2: RootJsonFormat[Res]
) extends BasicAccess(prefixPath) {
  //-----------------------[Http Requests]-----------------------//

  def get(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[Res] =
    exec.get[Res, ErrorApi.Error](urlBuilder.key(key).build())

  def delete(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[Unit] =
    exec.delete[ErrorApi.Error](urlBuilder.key(key).build())

  def list()(implicit executionContext: ExecutionContext): Future[Seq[Res]] =
    exec.get[Seq[Res], ErrorApi.Error](url)

  //-----------------------[Http Helpers]-----------------------//
  protected def post(creation: Creation)(implicit executionContext: ExecutionContext): Future[Res] =
    exec.post[Creation, Res, ErrorApi.Error](url, creation)

  protected def put(key: ObjectKey, updating: Updating)(implicit executionContext: ExecutionContext): Future[Res] =
    exec.put[Updating, Res, ErrorApi.Error](urlBuilder.key(key).build(), updating)

  /**
    * this is not a public method since we encourage users to call Query to set up parameters via fluent pattern.
    * @param request request
    * @param executionContext thread pool
    * @return results
    */
  protected def list(request: QueryRequest)(implicit executionContext: ExecutionContext): Future[Seq[Res]] =
    if (request.raw.isEmpty) list() else exec.get[Seq[Res], ErrorApi.Error](urlBuilder.params(request.raw).build())
}
