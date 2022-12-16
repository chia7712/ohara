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

package oharastream.ohara.configurator

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives.{complete, get, path, _}
import akka.http.scaladsl.{Http, server}
import oharastream.ohara.agent.ServiceCollie
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.configurator.Configurator.Mode
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestConfiguratorBuilder extends OharaTest {
  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(60, TimeUnit.SECONDS))

  @Test
  def nullHomeFolder(): Unit = an[NullPointerException] should be thrownBy Configurator.builder.homeFolder(null)

  @Test
  def emptyHomeFolder(): Unit = an[IllegalArgumentException] should be thrownBy Configurator.builder.homeFolder("")

  @Test
  def autoMkdirForHomeFolder(): Unit = {
    val folder = CommonUtils.createTempFolder(CommonUtils.randomString(10))
    folder.delete() shouldBe true
    folder.exists() shouldBe false
    Configurator.builder.homeFolder(folder.getCanonicalPath)
    folder.exists() shouldBe true
  }

  @Test
  def duplicatePort(): Unit = an[IllegalArgumentException] should be thrownBy Configurator.builder.port(10).port(20)

  @Test
  def testFakeCluster(): Unit = {
    Seq(
      (1, 1),
      (1, 2),
      (2, 1),
      (10, 10)
    ).foreach {
      case (numberOfBrokers, numberOfWorkers) =>
        val configurator = Configurator.builder.fake(numberOfBrokers, numberOfWorkers).build()
        try {
          result(configurator.serviceCollie.brokerCollie.clusters()).size shouldBe numberOfBrokers
          result(configurator.serviceCollie.workerCollie.clusters()).size shouldBe numberOfWorkers
          result(configurator.serviceCollie.clusters())
          // one broker generates one zk cluster
          .size shouldBe (numberOfBrokers + numberOfBrokers + numberOfWorkers)
          val nodes = result(configurator.store.values[Node]())
          nodes.isEmpty shouldBe false
          result(configurator.serviceCollie.clusters())
            .foreach(clusterStatus => clusterStatus.nodeNames.foreach(n => nodes.exists(_.hostname == n) shouldBe true))
        } finally configurator.close()
    }
  }

  @Test
  def createWorkerClusterWithoutBrokerCluster(): Unit = {
    an[IllegalArgumentException] should be thrownBy Configurator.builder.fake(0, 1)
  }

  @Test
  def createFakeConfiguratorWithoutClusters(): Unit = {
    val configurator = Configurator.builder.fake(0, 0).build()
    try result(configurator.serviceCollie.clusters()).size shouldBe 0
    finally configurator.close()
  }

  @Test
  def reassignServiceCollieAfterFake(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
    // in fake mode, a fake collie will be created
      .fake(1, 1)
      .serviceCollie(Mockito.mock(classOf[ServiceCollie]))
      .build()

  @Test
  def reassignServiceCollie(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .serviceCollie(Mockito.mock(classOf[ServiceCollie]))
      .serviceCollie(Mockito.mock(classOf[ServiceCollie]))
      .build()

  @Test
  def reassignHostname(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .hostname(CommonUtils.hostname())
      .hostname(CommonUtils.hostname())
      .build()

  @Test
  def reassignPort(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .port(CommonUtils.availablePort())
      .port(CommonUtils.availablePort())
      .build()

  @Test
  def reassignHomeFolder(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(10)).getCanonicalPath)
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(10)).getCanonicalPath)
      .build()

  @Test
  def reassignHomeFolderAfterFake(): Unit =
    an[IllegalArgumentException] should be thrownBy Configurator.builder
    // in fake mode, we have created a store
      .fake(1, 1)
      // you can't change the folder of store now
      .homeFolder(CommonUtils.createTempFolder(CommonUtils.randomString(10)).getCanonicalPath)
      .build()

  private[this] def toServer(route: server.Route): SimpleServer = {
    implicit val system: ActorSystem = ActorSystem("my-system")
    val server                       = Await.result(Http().bindAndHandle(route, "localhost", 0), Duration(30, TimeUnit.SECONDS))

    new SimpleServer {
      override def hostname: String = server.localAddress.getHostString
      override def port: Int        = server.localAddress.getPort
      override def close(): Unit = {
        Await.result(server.unbind(), Duration(30, TimeUnit.SECONDS))
        Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
      }
    }
  }

  @Test
  def testBuild(): Unit = {
    val apiServer           = k8sServer("default", "pod", "log")
    val configuratorBuilder = Configurator.builder
    val configurator        = configuratorBuilder.k8sServer(apiServer.url).k8sNamespace("default").build()
    configurator.mode shouldBe Mode.K8S
  }

  private[this] def k8sServer(namespace: String, podName: String, logMessage: String): SimpleServer = {
    val podsInfo = s"""
                                    |{"items": [
                                    |    {
                                    |      "metadata": {
                                    |        "name": "$podName",
                                    |        "labels": {
                                    |          "createdByOhara": "k8s"
                                    |        },
                                    |        "uid": "0f7200b8-c3c1-11e9-8e80-8ae0e3c47d1e",
                                    |        "creationTimestamp": "2019-08-21T03:09:16Z"
                                    |      },
                                    |      "spec": {
                                    |        "containers": [
                                    |          {
                                    |            "name": "ohara",
                                    |            "image": "ghcr.io/chia7712/ohara/broker:${VersionUtils.VERSION}",
                                    |            "ports": [
                                    |              {
                                    |                "hostPort": 43507,
                                    |                "containerPort": 43507,
                                    |                "protocol": "TCP"
                                    |              }]
                                    |          }
                                    |        ],
                                    |        "nodeName": "ohara-jenkins-it-00",
                                    |        "hostname": "057aac6a97-bk-c720992-ohara-jenkins-it-00"
                                    |      },
                                    |      "status": {
                                    |        "phase": "Running",
                                    |        "conditions": [
                                    |          {
                                    |            "type": "Ready",
                                    |            "status": "True",
                                    |            "lastProbeTime": null,
                                    |            "lastTransitionTime": "2019-08-21T03:09:18Z"
                                    |          }
                                    |        ]
                                    |      }
                                    |    }
                                    |  ]
                                    |}
       """.stripMargin
    toServer {
      path("namespaces" / namespace / "pods") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, podsInfo)))
        }
      } ~ path("namespaces" / namespace / "pods" / podName / "log") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, logMessage)))
        }
      } ~
        path("nodes") {
          get {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, k8sNodesResponse)))
          }
        }
    }
  }

  private[this] val k8sNodesResponse = s"""
        |{"items": [
        |    {
        |      "metadata": {
        |        "name": "node1"
        |      },
        |      "status": {
        |        "conditions": [
        |          {
        |            "type": "Ready",
        |            "status": "True",
        |            "lastHeartbeatTime": "2019-05-14T06:14:46Z",
        |            "lastTransitionTime": "2019-04-15T08:21:11Z",
        |            "reason": "KubeletReady",
        |            "message": "kubelet is posting ready status"
        |          }
        |        ],
        |        "addresses": [
        |          {
        |            "type": "InternalIP",
        |            "address": "10.2.0.4"
        |          },
        |          {
        |            "type": "Hostname",
        |            "address": "ohara-it-02"
        |          }
        |        ],
        |        "images": [
        |          {
        |            "names": [
        |              "quay.io/coreos/etcd@sha256:ea49a3d44a50a50770bff84eab87bac2542c7171254c4d84c609b8c66aefc211",
        |              "quay.io/coreos/etcd:v3.3.9"
        |            ],
        |            "sizeBytes": 39156721
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

  trait SimpleServer extends Releasable {
    def hostname: String
    def port: Int

    def url: String = s"http://$hostname:$port"
  }
}
