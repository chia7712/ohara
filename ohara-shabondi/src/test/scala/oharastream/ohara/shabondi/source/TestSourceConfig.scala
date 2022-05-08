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

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.TopicKey
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import org.scalatest.matchers.should.Matchers._

final class TestSourceConfig extends OharaTest {
  private def topicKey1 = TopicKey.of("default", "topic1")
  private def topicKey2 = TopicKey.of("default", "topic2")

  @Test
  def test(): Unit = {
    import oharastream.ohara.shabondi.ShabondiDefinitions._
    val jsonSourceTopicKeys = TopicKey.toJsonString(Seq(topicKey1, topicKey2).asJava)
    val args = Map(
      CLIENT_PORT_DEFINITION.key      -> "8080",
      SOURCE_TO_TOPICS_DEFINITION.key -> jsonSourceTopicKeys
    )
    val config = new SourceConfig(args)
    config.port should ===(8080)

    val topicKeys = Seq(TopicKey.of("default", "topic1"), TopicKey.of("default", "topic2"))

    config.sourceToTopics.size should ===(2)
    config.sourceToTopics(0) should ===(topicKeys(0))
    config.sourceToTopics(1) should ===(topicKeys(1))
  }
}
