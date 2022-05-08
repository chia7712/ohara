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

package oharastream.ohara.configurator.fake

import java.util.concurrent.{CompletableFuture, CompletionStage, ConcurrentHashMap}
import java.{lang, util}

import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.kafka.{TopicAdmin, TopicCreator, TopicDescription, TopicOption}

private[configurator] class FakeTopicAdmin extends TopicAdmin {
  import scala.jdk.CollectionConverters._

  override val connectionProps: String = "Unknown"

  private[this] val cachedTopics = new ConcurrentHashMap[TopicKey, TopicDescription]()

  override def createPartitions(topicKey: TopicKey, numberOfPartitions: Int): CompletionStage[Void] = {
    val previous = cachedTopics.get(topicKey)
    val f        = new CompletableFuture[Void]()
    if (previous == null)
      f.completeExceptionally(
        new NoSuchElementException(
          s"the topic:$topicKey doesn't exist. actual:${cachedTopics.keys().asScala.mkString(",")}"
        )
      )
    else {
      cachedTopics.put(
        topicKey,
        new TopicDescription(
          previous.topicKey,
          previous.partitionInfos(),
          previous.options
        )
      )
    }
    f
  }

  override def topicKeys: CompletionStage[util.Set[TopicKey]] = CompletableFuture.completedFuture(cachedTopics.keySet())

  override def topicDescription(key: TopicKey): CompletionStage[TopicDescription] = {
    val topic = cachedTopics.get(key)
    val f     = new CompletableFuture[TopicDescription]()
    if (topic == null) f.completeExceptionally(new NoSuchElementException(s"$key does not exist"))
    else f.complete(topic)
    f
  }

  override def topicCreator(): TopicCreator =
    (_: Int, _: Short, options: util.Map[String, String], topicKey: TopicKey) => {
      val f = new CompletableFuture[Void]()
      if (cachedTopics.contains(topicKey))
        f.completeExceptionally(new IllegalArgumentException(s"$topicKey already exists!"))
      else {
        val topicInfo = new TopicDescription(
          topicKey,
          java.util.List.of(),
          options.asScala
            .map {
              case (key, value) =>
                new TopicOption(
                  key,
                  value,
                  false,
                  false,
                  false
                )
            }
            .toSeq
            .asJava
        )
        if (cachedTopics.putIfAbsent(topicKey, topicInfo) != null)
          throw new RuntimeException(s"the $topicKey already exists in kafka")
        f.complete(null)
      }
      f
    }

  private[this] var _closed = false

  override def close(): Unit = _closed = true

  override def closed(): Boolean = _closed

  override def brokerPorts(): CompletionStage[util.Map[String, Integer]] =
    CompletableFuture.completedFuture(java.util.Map.of())

  override def exist(topicKey: TopicKey): CompletionStage[lang.Boolean] =
    CompletableFuture.completedFuture(cachedTopics.containsKey(topicKey))

  override def deleteTopic(topicKey: TopicKey): CompletionStage[lang.Boolean] = {
    val f       = new CompletableFuture[lang.Boolean]()
    val removed = cachedTopics.remove(topicKey)
    if (removed == null) f.complete(false)
    else f.complete(true)
    f
  }
}
