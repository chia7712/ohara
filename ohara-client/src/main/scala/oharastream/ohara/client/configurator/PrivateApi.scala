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

import oharastream.ohara.client.HttpExecutor
import oharastream.ohara.client.configurator.ErrorApi._
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}
object PrivateApi {
  val PREFIX = "_private"

  case class Deletion(groups: Set[String], kinds: Set[String])

  implicit val DELETION_FORMAT: RootJsonFormat[Deletion] = jsonFormat2(Deletion)

  def delete(hostname: String, port: Int, deletion: Deletion)(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    HttpExecutor.SINGLETON.delete[Deletion, Error](url = s"http://$hostname:$port/$PREFIX", request = deletion)
}
