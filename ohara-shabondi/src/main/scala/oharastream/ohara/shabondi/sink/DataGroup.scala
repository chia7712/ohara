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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import oharastream.ohara.common.util.Releasable
import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.setting.{ObjectKey, TopicKey}
import oharastream.ohara.metrics.basic.Counter

private[sink] class DataGroup(
  val name: String,
  objectKey: ObjectKey,
  brokerProps: String,
  topicKeys: Set[TopicKey],
  pollTimeout: JDuration
) extends Releasable {
  private val log = Logger(classOf[RowQueue])

  private val rowCounter: Counter =
    Counter.builder
      .key(objectKey)
      .item(s"rows-$name")
      .unit("row")
      .document(s"The number of received rows of group $name")
      .value(0)
      .register()

  val queue                = new RowQueue
  val queueProducer        = new QueueProducer(name, queue, brokerProps, topicKeys, pollTimeout, rowCounter)
  private[this] val closed = new AtomicBoolean(false)

  def resume(): Unit =
    if (!closed.get) {
      queueProducer.resume()
    }

  def pause(): Unit =
    if (!closed.get) {
      queueProducer.pause()
    }

  def isIdle(idleTime: JDuration): Boolean = queue.isIdle(idleTime)

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      var exception: Throwable = null
      val addSuppressedException: Consumer[Throwable] = (ex: Throwable) => {
        if (exception == null) exception = ex else exception.addSuppressed(ex)
      }
      Releasable.close(queueProducer, addSuppressedException)
      Releasable.close(rowCounter, addSuppressedException)
      if (exception != null) throw exception
      log.info("Group {} closed.", name)
    }
  }
}
