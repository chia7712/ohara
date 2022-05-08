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

package oharastream.ohara.shabondi

import java.time.Duration

import akka.http.scaladsl.testkit.ScalatestRouteTest
import oharastream.ohara.common.data.{Cell, Row, Serializer}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.{Consumer, Producer}
import org.scalatest.Suite
import org.scalatest.concurrent.ScalaFutures

import scala.collection.{AbstractIterator, immutable}
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

private[shabondi] object ShabondiRouteTestSupport extends Suite with ScalaFutures with ScalatestRouteTest

private[shabondi] object KafkaSupport {
  import oharastream.ohara.shabondi.common.ConvertSupport._

  import scala.jdk.CollectionConverters._

  def newProducer(brokers: String): Producer[Row, Array[Byte]] =
    Producer
      .builder()
      .connectionProps(brokers)
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .build()

  def newConsumer(brokers: String, topicKey: TopicKey): Consumer[Row, Array[Byte]] =
    Consumer
      .builder()
      .keySerializer(Serializer.ROW)
      .valueSerializer(Serializer.BYTES)
      .offsetFromBegin()
      .topicKey(topicKey)
      .connectionProps(brokers)
      .build()

  def sendRows(
    producer: Producer[Row, Array[Byte]],
    topicKey: TopicKey,
    delayMillis: Long,
    f: () => Iterator[Row]
  )(implicit ec: ExecutionContext): (Promise[Boolean], Future[Unit]) = {
    val cancelled       = Promise[Boolean]()
    val cancelledFuture = cancelled.future
    val future = Future {
      if (delayMillis > 0) Thread.sleep(delayMillis)
      val rowIterator = f()
      while (!cancelledFuture.isCompleted && rowIterator.hasNext) {
        producer
          .sender()
          .key(rowIterator.next())
          .topicKey(topicKey)
          .send
          .toScala
        Thread.sleep(2)
      }
    }
    (cancelled, future)
  }

  def cycleSendRows(producer: Producer[Row, Array[Byte]], topicKey: TopicKey, f: () => Iterator[Row])(
    implicit ec: ExecutionContext
  ): (Promise[Boolean], Future[Unit]) = {
    val iterator = Iterator.continually {
      val it = f()
      if (it.isEmpty) throw new IllegalArgumentException("empty iterator") else it
    }.flatten
    sendRows(producer, topicKey, 0, () => iterator)
  }

  def pollTopicOnce(
    brokers: String,
    topicKey: TopicKey,
    timeoutSecond: Long,
    expectedSize: Int
  ): Seq[Consumer.Record[Row, Array[Byte]]] = {
    val consumer = KafkaSupport.newConsumer(brokers, topicKey)
    try {
      consumer.poll(Duration.ofSeconds(timeoutSecond), expectedSize).asScala.toSeq
    } finally {
      Releasable.close(consumer)
    }
  }

  private def rowInerator(len: Int)(elem: => Row): Iterator[Row] =
    new AbstractIterator[Row] {
      private var i        = 0
      def hasNext: Boolean = i < len
      def next(): Row =
        if (hasNext) {
          i += 1; elem
        } else Iterator.empty.next()
    }

  def singleRow(columnSize: Int, rowId: Int = 0) =
    Row.of(
      (1 to columnSize).map(ci => Cell.of(s"col-$ci", s"r$rowId-${ci * 10}")): _*
    )

  def multipleRows(rowSize: Int): immutable.Iterable[Row] =
    (0 until rowSize).map { ri =>
      singleRow(10, ri)
    }

  def prepareBulkOfRow(
    brokerProps: String,
    topicKey: TopicKey,
    rowCount: Int,
    duration: FiniteDuration = FiniteDuration(10, SECONDS)
  )(implicit ec: ExecutionContext): Unit = {
    val producer = KafkaSupport.newProducer(brokerProps)
    try {
      var rowId = -1
      val (_, sendRowsFuture) = KafkaSupport.sendRows(producer, topicKey, 0, () => {
        rowInerator(rowCount) {
          rowId = rowId + 1;
          singleRow(3, rowId)
        }
      })
      Await.result(sendRowsFuture, duration)
    } finally {
      Releasable.close(producer)
    }
  }
}
