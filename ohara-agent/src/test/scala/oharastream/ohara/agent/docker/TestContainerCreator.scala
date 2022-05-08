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

package oharastream.ohara.agent.docker

import java.util.Objects

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.{ExecutionContext, Future}

class TestContainerCreator extends OharaTest {
  private[this] def fake(): DockerClient.ContainerCreator =
    (
      nodeName: String,
      hostname: String,
      imageName: String,
      volumeMaps: Map[String, String],
      name: String,
      command: Option[String],
      arguments: Seq[String],
      ports: Map[Int, Int],
      envs: Map[String, String],
      routes: Map[String, String],
      _: ExecutionContext
    ) =>
      Future.successful {
        // we check only the required arguments
        CommonUtils.requireNonEmpty(nodeName)
        CommonUtils.requireNonEmpty(hostname)
        CommonUtils.requireNonEmpty(imageName)
        CommonUtils.requireNonEmpty(name)
        Objects.requireNonNull(command)
        Objects.requireNonNull(ports)
        Objects.requireNonNull(envs)
        Objects.requireNonNull(routes)
        Objects.requireNonNull(arguments)
        Objects.requireNonNull(volumeMaps)
      }

  @Test
  def nullHostname(): Unit = an[NullPointerException] should be thrownBy fake().hostname(null)

  @Test
  def emptyHostname(): Unit = an[IllegalArgumentException] should be thrownBy fake().hostname("")

  @Test
  def nullImageName(): Unit = an[NullPointerException] should be thrownBy fake().imageName(null)

  @Test
  def emptyImageName(): Unit = an[IllegalArgumentException] should be thrownBy fake().imageName("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy fake().name(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy fake().name("")

  @Test
  def nullCommand(): Unit = an[NullPointerException] should be thrownBy fake().command(null)

  @Test
  def emptyCommand(): Unit = fake().command("")

  @Test
  def nullPorts(): Unit = an[NullPointerException] should be thrownBy fake().portMappings(null)

  @Test
  def emptyPorts(): Unit = fake().portMappings(Map.empty)

  @Test
  def nullEnvs(): Unit = an[NullPointerException] should be thrownBy fake().envs(null)

  @Test
  def emptyEnvs(): Unit = fake().envs(Map.empty)

  @Test
  def nullRoute(): Unit = an[NullPointerException] should be thrownBy fake().routes(null)

  @Test
  def emptyRoute(): Unit = fake().routes(Map.empty)

  @Test
  def nullArguments(): Unit = an[NullPointerException] should be thrownBy fake().arguments(null)

  @Test
  def emptyArguments(): Unit = fake().arguments(Seq.empty)

  @Test
  def testExecuteWithoutRequireArguments(): Unit =
    // At least assign imageName
    an[NullPointerException] should be thrownBy fake().create()
}
