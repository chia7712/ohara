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

package oharastream.ohara.configurator.route

import akka.http.scaladsl.server
import oharastream.ohara.client.configurator.ObjectApi
import oharastream.ohara.client.configurator.ObjectApi.{Creation, ObjectInfo, PREFIX, Updating}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.store.DataStore

import scala.concurrent.{ExecutionContext, Future}

private[configurator] object ObjectRoute {
  private[this] def toObject(creation: Creation): Future[ObjectInfo] =
    Future.successful(
      ObjectInfo(
        creation.raw,
        // add the last timestamp manually since there is no explicit field in ObjectInfo
        CommonUtils.current()
      )
    )

  def apply(implicit store: DataStore, executionContext: ExecutionContext): server.Route =
    RouteBuilder[Creation, Updating, ObjectInfo]()
      .prefix(PREFIX)
      .hookOfCreation(toObject)
      .hookOfUpdating(
        (key, updating, previousOption) =>
          toObject(previousOption match {
            case None => new Creation(updating.raw)
            case Some(previous) =>
              ObjectApi.access.request.key(key).settings(previous.settings).settings(updating.raw).creation
          })
      )
      .hookOfGet(Future.successful(_))
      .hookOfList(Future.successful(_))
      .hookBeforeDelete(_ => Future.unit)
      .build()
}
