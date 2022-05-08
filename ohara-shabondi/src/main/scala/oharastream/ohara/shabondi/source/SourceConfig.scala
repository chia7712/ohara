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

import oharastream.ohara.common.setting.{ObjectKey, TopicKey}
import oharastream.ohara.shabondi.ShabondiDefinitions._

import scala.jdk.CollectionConverters._

private[shabondi] class SourceConfig(raw: Map[String, String]) {
  def group: String = raw(GROUP_DEFINITION.key)

  def name: String = raw(NAME_DEFINITION.key)

  def objectKey: ObjectKey = ObjectKey.of(group, name)

  def shabondiClass: String = raw(SHABONDI_CLASS_DEFINITION.key)

  def port: Int = raw(CLIENT_PORT_DEFINITION.key).toInt

  def brokers: String = raw(BROKERS_DEFINITION.key)

  def sourceToTopics: Seq[TopicKey] = TopicKey.toTopicKeys(raw(SOURCE_TO_TOPICS_DEFINITION.key)).asScala.toSeq
}
