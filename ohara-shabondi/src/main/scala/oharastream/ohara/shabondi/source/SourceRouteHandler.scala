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

package oharastream.ohara.shabondi.source

import java.util.concurrent.{ExecutorService, Executors}
import java.util.function.Consumer

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.Producer
import oharastream.ohara.metrics.basic.Counter
import oharastream.ohara.shabondi.common.JsonSupport.RowData
import oharastream.ohara.shabondi.common.{ConvertSupport, JsonSupport, RouteHandler, ShabondiUtils}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

private[shabondi] object SourceRouteHandler {
  def apply(config: SourceConfig)(implicit actorSystem: ActorSystem) =
    new SourceRouteHandler(config)
}
private[shabondi] class SourceRouteHandler(
  config: SourceConfig
)(implicit actorSystem: ActorSystem)
    extends RouteHandler {
  private val log = Logging(actorSystem, classOf[SourceRouteHandler])

  private val threadPool: ExecutorService                  = Executors.newFixedThreadPool(4)
  implicit private val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(threadPool)

  private val totalRowsCounter =
    Counter.builder
      .key(config.objectKey)
      .item("total-rows")
      .unit("row")
      .document("The number of received rows")
      .value(0)
      .register()

  private val exceptionHandler = ExceptionHandler {
    case ex: Throwable =>
      log.error(ex, ex.getMessage)
      complete((StatusCodes.InternalServerError, ex.getMessage))
  }

  private val producer = Producer
    .builder()
    .connectionProps(config.brokers)
    .keySerializer(Serializer.ROW)
    .valueSerializer(Serializer.BYTES)
    .build()

  private val topicKeys = config.sourceToTopics

  // TODO: The error handling for the Kafka Producer exception
  private val sendRowFlow = Flow[RowData].mapAsync(4) { rowData =>
    import ConvertSupport._
    val row = JsonSupport.toRow(rowData)
    Future.sequence(topicKeys.map { topicKey =>
      val sender = producer.sender().key(row).topicKey(topicKey)
      sender.send.toScala
    })
  }

  private val rowQueue = Source
    .queue[RowData](1024, OverflowStrategy.backpressure)
    .via(sendRowFlow)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  override def route(): Route = handleExceptions(exceptionHandler) {
    pathEndOrSingleSlash {
      post {
        entity(as[RowData]) { rowData =>
          totalRowsCounter.incrementAndGet()
          rowQueue.offer(rowData)
          complete(StatusCodes.OK)
        } ~ {
          complete(
            StatusCodes.BadRequest -> s"Invalid format of request body, please reference: ${ShabondiUtils.apiUrl}"
          )
        }
      } ~ {
        complete(StatusCodes.MethodNotAllowed -> s"Unsupported method, please reference: ${ShabondiUtils.apiUrl}")
      }
    }
  } // handleExceptions

  override def close(): Unit = {
    var exception: Throwable = null
    val addSuppressedException: Consumer[Throwable] = (ex: Throwable) => {
      if (exception == null) exception = ex else exception.addSuppressed(ex)
    }
    Releasable.close(producer, addSuppressedException)
    Releasable.close(totalRowsCounter, addSuppressedException)
    if (exception != null) throw exception
    threadPool.shutdown()
  }
}
