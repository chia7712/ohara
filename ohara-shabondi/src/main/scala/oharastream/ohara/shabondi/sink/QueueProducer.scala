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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Queue => JQueue}

import oharastream.ohara.common.data.{Row, Serializer}
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.kafka.Consumer
import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.metrics.basic.Counter

import scala.jdk.CollectionConverters._

private[sink] class QueueProducer(
  val groupName: String,
  val queue: JQueue[Row],
  val brokerProps: String,
  val topicKeys: Set[TopicKey],
  val pollTimeout: JDuration,
  val rowCounter: Counter
) extends Runnable
    with Releasable {
  private[this] val log                    = Logger(classOf[QueueProducer])
  private[this] val paused: AtomicBoolean  = new AtomicBoolean(false)
  private[this] val stopped: AtomicBoolean = new AtomicBoolean(false)

  private[this] val consumer: Consumer[Row, Array[Byte]] = Consumer
    .builder()
    .keySerializer(Serializer.ROW)
    .valueSerializer(Serializer.BYTES)
    .offsetFromBegin()
    .topicKeys(topicKeys.asJava)
    .connectionProps(brokerProps)
    .build()

  override def run(): Unit = {
    log.info(
      "{} group `{}` start.(topics={}, brokerProps={})",
      this.getClass.getSimpleName,
      groupName,
      topicKeys.mkString(","),
      brokerProps
    )
    try {
      while (!stopped.get) {
        if (!paused.get && queue.isEmpty) {
          val rows = consumer.poll(pollTimeout).asScala.map(_.key.get)
          rows.foreach { r =>
            queue.add(r)
            rowCounter.incrementAndGet()
          }
          log.trace("    group[{}], queue: {}, rows: {}", groupName, queue.size, rows.size)
        } else {
          TimeUnit.MILLISECONDS.sleep(10)
        }
      } // while
    } finally {
      consumer.close()
      log.info("stopped.")
    }
  }

  override def close(): Unit = {
    stop()
  }

  def stop(): Unit = {
    stopped.set(true)
  }

  def pause(): Unit = {
    if (paused.compareAndSet(false, true)) {
      log.info("{} paused.", this.getClass.getSimpleName)
    }
  }

  def resume(): Unit = {
    if (paused.compareAndSet(true, false)) {
      log.info("{} resumed.", this.getClass.getSimpleName)
    }
  }
}
