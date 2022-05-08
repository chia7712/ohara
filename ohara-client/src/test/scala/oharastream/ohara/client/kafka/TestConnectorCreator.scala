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

package oharastream.ohara.client.kafka

import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestConnectorCreator extends OharaTest {
  private[this] val notWorkingClient = ConnectorAdmin("localhost:2222")

  @Test
  def nullConfigs(): Unit =
    an[NullPointerException] should be thrownBy notWorkingClient.connectorCreator().settings(null)

  @Test
  def nullSchema(): Unit = an[NullPointerException] should be thrownBy notWorkingClient.connectorCreator().columns(null)

  @Test
  def nullConnectorKey(): Unit =
    an[NullPointerException] should be thrownBy notWorkingClient.connectorCreator().connectorKey(null)

  @Test
  def illegalNumberOfTasks(): Unit =
    an[IllegalArgumentException] should be thrownBy notWorkingClient.connectorCreator().numberOfTasks(-1)

  @Test
  def nullClass(): Unit =
    an[NullPointerException] should be thrownBy notWorkingClient
      .connectorCreator()
      .connectorClass(null.asInstanceOf[Class[_]])

  @Test
  def nullClassName(): Unit =
    an[NullPointerException] should be thrownBy notWorkingClient.connectorCreator().className(null.asInstanceOf[String])

  @Test
  def emptyClassName(): Unit =
    an[IllegalArgumentException] should be thrownBy notWorkingClient.connectorCreator().className("")

  @Test
  def nullTopicKey(): Unit =
    an[NullPointerException] should be thrownBy notWorkingClient.connectorCreator().topicKey(null)

  @Test
  def nullTopicKeys(): Unit =
    an[NullPointerException] should be thrownBy notWorkingClient.connectorCreator().topicKeys(null)

  @Test
  def emptyTopicKeys(): Unit =
    an[IllegalArgumentException] should be thrownBy notWorkingClient.connectorCreator().topicKeys(Set.empty)
}
