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

import java.util
import java.util.concurrent.{ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.data.Row
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.TopicAdmin
import oharastream.ohara.shabondi.common.ShabondiUtils
import oharastream.ohara.shabondi.sink.SinkConfig
import oharastream.ohara.shabondi.source.SourceConfig
import oharastream.ohara.testing.WithBroker
import org.junit.jupiter.api.AfterEach

import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[shabondi] abstract class BasicShabondiTest extends WithBroker {
  protected val log = Logger(this.getClass())

  protected val brokerProps            = testUtil.brokersConnProps
  protected val topicAdmin: TopicAdmin = TopicAdmin.of(brokerProps)

  protected val newThreadPool: () => ExecutorService = () =>
    Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(this.getClass.getSimpleName + "-").build())

  protected val countRows: (util.Queue[Row], Long, ExecutionContext) => Future[Long] =
    (queue, executionTime, ec) =>
      Future {
        log.debug("countRows begin...")
        val baseTime = System.currentTimeMillis()
        var count    = 0L
        var running  = true
        while (running) {
          val row = queue.poll()
          if (row != null) count += 1 else Thread.sleep(100)
          running = (System.currentTimeMillis() - baseTime) < executionTime
        }
        log.debug("countRows done")
        count
      }(ec)

  protected def createTopicKey = TopicKey.of("default", CommonUtils.randomString(5))

  protected def createTestTopic(topicKey: TopicKey): Unit =
    topicAdmin.topicCreator
      .numberOfPartitions(1)
      .numberOfReplications(1.toShort)
      .topicKey(topicKey)
      .create

  protected def defaultSourceConfig(
    sourceToTopics: Seq[TopicKey] = Seq.empty[TopicKey]
  ): SourceConfig = {
    import ShabondiDefinitions._
    val args = mutable.ArrayBuffer(
      GROUP_DEFINITION.key + "=" + CommonUtils.randomString(5),
      NAME_DEFINITION.key + "=" + CommonUtils.randomString(3),
      SHABONDI_CLASS_DEFINITION.key + "=" + classOf[ShabondiSource].getName,
      CLIENT_PORT_DEFINITION.key + "=8080",
      BROKERS_DEFINITION.key + "=" + testUtil.brokersConnProps
    )
    if (sourceToTopics.nonEmpty)
      args += s"${SOURCE_TO_TOPICS_DEFINITION.key}=${TopicKey.toJsonString(sourceToTopics.asJava)}"

    val rawConfig = ShabondiUtils.parseArgs(args.toArray)
    new SourceConfig(rawConfig)
  }

  protected def defaultSinkConfig(
    sinkFromTopics: Seq[TopicKey] = Seq.empty[TopicKey]
  ): SinkConfig = {
    import ShabondiDefinitions._
    val args = mutable.ArrayBuffer(
      GROUP_DEFINITION.key + "=" + CommonUtils.randomString(5),
      NAME_DEFINITION.key + "=" + CommonUtils.randomString(3),
      SHABONDI_CLASS_DEFINITION.key + "=" + classOf[ShabondiSink].getName,
      CLIENT_PORT_DEFINITION.key + "=8080",
      BROKERS_DEFINITION.key + "=" + testUtil.brokersConnProps
    )
    if (sinkFromTopics.nonEmpty)
      args += s"${SINK_FROM_TOPICS_DEFINITION.key}=${TopicKey.toJsonString(sinkFromTopics.asJava)}"
    val rawConfig = ShabondiUtils.parseArgs(args.toArray)
    new SinkConfig(rawConfig)
  }

  protected def singleRow(columnSize: Int, rowId: Int = 0): Row =
    KafkaSupport.singleRow(columnSize, rowId)

  protected def multipleRows(rowSize: Int): immutable.Iterable[Row] =
    KafkaSupport.multipleRows(rowSize)

  @AfterEach
  def tearDown(): Unit = Releasable.close(topicAdmin)
}
