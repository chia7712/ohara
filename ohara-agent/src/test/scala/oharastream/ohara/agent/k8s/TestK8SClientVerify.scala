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

package oharastream.ohara.agent.k8s

import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.fake.FakeK8SClient
import oharastream.ohara.agent.{DataCollie, ServiceCollie}
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class TestK8SClientVerify extends OharaTest {
  private[this] val dataCollie: DataCollie = DataCollie(Seq.empty)

  private[this] def node: Node = Node("ohara", "user", "password")

  @Test
  def testMockK8sClientVerifyNode1(): Unit = {
    val fakeK8SClient = new FakeK8SClient(true, Option(K8SStatusInfo(true, "")), "")
    val serviceCollie: ServiceCollie =
      ServiceCollie.k8sModeBuilder.dataCollie(dataCollie).k8sClient(fakeK8SClient).build()
    Await.result(
      serviceCollie.verifyNode(node),
      Duration(30, TimeUnit.SECONDS)
    ) shouldBe "ohara node is running."
  }

  @Test
  def testMockK8sClientVerifyNode2(): Unit = {
    val fakeK8SClient = new FakeK8SClient(true, Option(K8SStatusInfo(false, "node failed.")), "")
    val serviceCollie: ServiceCollie =
      ServiceCollie.k8sModeBuilder.dataCollie(dataCollie).k8sClient(fakeK8SClient).build()
    intercept[IllegalStateException] {
      Await.result(
        serviceCollie.verifyNode(node),
        Duration(30, TimeUnit.SECONDS)
      )
    }.getMessage shouldBe "ohara node doesn't running container. cause: node failed."
  }

  @Test
  def testMockK8sClientVerifyNode3(): Unit = {
    val fakeK8SClient = new FakeK8SClient(false, Option(K8SStatusInfo(false, "failed")), "")
    val serviceCollie: ServiceCollie =
      ServiceCollie.k8sModeBuilder.dataCollie(dataCollie).k8sClient(fakeK8SClient).build()
    intercept[IllegalStateException] {
      Await.result(
        serviceCollie.verifyNode(node),
        Duration(30, TimeUnit.SECONDS)
      )
    }.getMessage shouldBe "ohara node doesn't running container. cause: failed"
  }

  @Test
  def testMockK8SClientVerifyNode4(): Unit = {
    val fakeK8SClient = new FakeK8SClient(false, None, "")
    val serviceCollie: ServiceCollie =
      ServiceCollie.k8sModeBuilder.dataCollie(dataCollie).k8sClient(fakeK8SClient).build()
    intercept[IllegalStateException] {
      Await.result(
        serviceCollie.verifyNode(node),
        Duration(30, TimeUnit.SECONDS)
      )
    }.getMessage shouldBe "ohara node doesn't running container. cause: ohara node doesn't exists."
  }
}
