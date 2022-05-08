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

package oharastream.ohara.shabondi.sink

import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.data.Row
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.shabondi.common.{JsonSupport, RouteHandler, ShabondiUtils}
import spray.json.DefaultJsonProtocol._

import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.DurationConverters._
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

private[shabondi] object SinkRouteHandler {
  def apply(config: SinkConfig)(implicit actorSystem: ActorSystem) =
    new SinkRouteHandler(config)
}

private[shabondi] class SinkRouteHandler(config: SinkConfig)(implicit actorSystem: ActorSystem) extends RouteHandler {
  implicit private val contextExecutor: ExecutionContextExecutor = actorSystem.dispatcher

  private val log              = Logger(classOf[SinkRouteHandler])
  private[sink] val dataGroups = SinkDataGroups(config)

  def scheduleFreeIdleGroups(interval: JDuration, idleTime: JDuration): Unit =
    actorSystem.scheduler.scheduleWithFixedDelay(Duration(1, TimeUnit.SECONDS), interval.toScala) { () =>
      {
        log.trace("scheduled free group, total group: {} ", dataGroups.size)
        dataGroups.freeIdleGroup(idleTime)
      }
    }

  private val exceptionHandler = ExceptionHandler {
    case ex: Throwable =>
      log.error(ex.getMessage, ex)
      complete((StatusCodes.InternalServerError, ex.getMessage))
  }

  private def fullyPollQueue(queue: RowQueue): Seq[Row] = {
    val buffer    = ArrayBuffer.empty[Row]
    var item: Row = queue.poll()
    while (item != null) {
      buffer += item
      item = queue.poll()
    }
    buffer.toSeq
  }

  private def apiUrl = ShabondiUtils.apiUrl

  def route(): Route = handleExceptions(exceptionHandler) {
    path("groups" / Segment) { groupId =>
      get {
        if (CommonUtils.isAlphanumeric(groupId)) {
          val group  = dataGroups.createIfAbsent(groupId)
          val result = fullyPollQueue(group.queue).map(row => JsonSupport.toRowData(row))
          complete(result)
        } else {
          val entity =
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, "Illegal group name, only accept alpha and numeric.")
          complete(StatusCodes.NotAcceptable -> entity)
        }
      } ~ {
        complete(StatusCodes.MethodNotAllowed -> s"Unsupported method, please reference: $apiUrl")
      }
    } ~ {
      complete(StatusCodes.NotFound -> s"Please reference: $apiUrl")
    }
  }

  override def close(): Unit = {
    Releasable.close(dataGroups)
  }
}
