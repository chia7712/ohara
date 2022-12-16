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

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.http.scaladsl.{Http, server}
import oharastream.ohara.agent.k8s.K8SJson._
import oharastream.ohara.client.configurator.NodeApi.Resource
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class TestK8SClient extends OharaTest {
  private[this] val namespace: String = K8SClient.NAMESPACE_DEFAULT_VALUE

  private[this] val podsInfo = s"""
                    |{"items": [
                    |    {
                    |      "metadata": {
                    |        "name": "057aac6a97-bk-c720992",
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

  @Test
  def testApiServerURLNull(): Unit =
    an[NullPointerException] should be thrownBy {
      K8SClient.builder
        .build()
    }

  @Test
  def testApiServerURLEmpty(): Unit = {
    an[IllegalArgumentException] should be thrownBy {
      K8SClient.builder
        .serverURL("")
        .build()
    }
  }

  @Test
  def testApiServerURLNotNull(): Unit = {
    K8SClient.builder
      .serverURL("http://localhost:8080/api/v1")
      .build()
      .isInstanceOf[K8SClient] shouldBe true
  }

  @Test
  def testK8SClientBuildPattern(): Unit = {
    K8SClient.builder
      .metricsServerURL("http://localhost:8080/apis")
      .serverURL("http://localhsot:8080/api/v1")
      .build()
      .isInstanceOf[K8SClient] shouldBe true
  }

  @Test
  def testCreatorEnumator(): Unit = {
    ImagePullPolicy.ALWAYS.toString shouldBe "Always"
    ImagePullPolicy.IFNOTPRESENT.toString shouldBe "IfNotPresent"
    ImagePullPolicy.NEVER.toString shouldBe "Never"

    RestartPolicy.Always.toString shouldBe "Always"
    RestartPolicy.OnFailure.toString shouldBe "OnFailure"
    RestartPolicy.Never.toString shouldBe "Never"
  }

  @Test
  def testPullPolicyIFNOTPRESENT(): Unit = {
    val nodeName = "ohara-it-02"
    val podName  = "container1"
    val s        = imagePolicyURL(nodeName, podName, ImagePullPolicy.IFNOTPRESENT)
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      Await.result(
        client.containerCreator
          .name(podName)
          .imageName("hello world")
          .hostname("test1")
          .nodeName(nodeName)
          .threadPool(scala.concurrent.ExecutionContext.Implicits.global)
          .create(),
        Duration(30, TimeUnit.SECONDS)
      )
    } finally s.close()
  }

  @Test
  def testRestartPolicyDefault(): Unit = {
    val nodeName = "ohara-it-02"
    val podName  = "container1"
    val s        = imagePolicyURL(nodeName, podName, ImagePullPolicy.IFNOTPRESENT)
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      Await.result(
        client.containerCreator
          .name(podName)
          .imageName("hello world")
          .hostname("test1")
          .nodeName(nodeName)
          .threadPool(scala.concurrent.ExecutionContext.Implicits.global)
          .create(),
        Duration(30, TimeUnit.SECONDS)
      )
    } finally s.close()
  }

  @Test
  def testPullPolicyIsAlways(): Unit = {
    val nodeName = "ohara-it-02"
    val podName  = "container1"
    val s        = imagePolicyURL(nodeName, podName, ImagePullPolicy.ALWAYS)
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      Await.result(
        client.containerCreator
          .name(podName)
          .imageName("hello world")
          .hostname("test1")
          .pullImagePolicy(ImagePullPolicy.ALWAYS)
          .nodeName(nodeName)
          .create(),
        Duration(30, TimeUnit.SECONDS)
      )
    } finally s.close()
  }

  @Test
  def testPullPolicyIsNever(): Unit = {
    val nodeName = "ohara-it-02"
    val podName  = "container1"
    val s        = imagePolicyURL(nodeName, podName, ImagePullPolicy.NEVER)
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      Await.result(
        client.containerCreator
          .name(podName)
          .imageName("hello world")
          .hostname("test1")
          .pullImagePolicy(ImagePullPolicy.NEVER)
          .nodeName(nodeName)
          .create(),
        Duration(30, TimeUnit.SECONDS)
      )
    } finally s.close()
  }

  @Test
  def testPullPolicyNotSetting(): Unit = {
    val nodeName = "ohara-it-02"
    val podName  = "container1"
    val s        = imagePolicyURL(nodeName, podName, ImagePullPolicy.IFNOTPRESENT)
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      Await.result(
        client.containerCreator
          .name(podName)
          .imageName("hello world")
          .hostname("test1")
          .nodeName(nodeName)
          .create(),
        Duration(30, TimeUnit.SECONDS)
      )
    } finally s.close()
  }

  @Test
  def testImages(): Unit = {
    val node   = CommonUtils.randomString()
    val images = Seq(CommonUtils.randomString(), CommonUtils.randomString())
    val plain  = s"""
                 |{
                 |  "items": [
                 |    {
                 |      "status": {
                 |        "addresses": [],
                 |        "images": [
                 |          {
                 |            "names": [${images.map(s => "\"" + s + "\"").mkString(",")}]
                 |          }
                 |        ],
                 |        "conditions": []
                 |      },
                 |      "metadata": {
                 |        "name": "$node"
                 |      }
                 |    }
                 |  ]
                 |}
               """.stripMargin

    // test communication
    val s = toServer {
      path("nodes") {
        get {
          complete(plain.parseJson)
        }
      }
    }
    try {
      val client           = K8SClient.builder.serverURL(s.url).build()
      val imagesFromServer = Await.result(client.imageNames(), Duration(30, TimeUnit.SECONDS))
      imagesFromServer shouldBe Map(node -> images)
    } finally s.close()
  }

  @Test
  def testForceRemovePod(): Unit = {
    val s         = forceRemovePodURL("057aac6a97-bk-c720992")
    val k8sClient = K8SClient.builder.serverURL(s.url).build()
    try Await.result(k8sClient.forceRemove("057aac6a97-bk-c720992"), Duration(30, TimeUnit.SECONDS))
    finally s.close()
  }

  @Test
  def testLog(): Unit = {
    val podName = "057aac6a97-bk-c720992"
    val s       = log(podName)
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      Await.result(client.log(podName, None), Duration(10, TimeUnit.SECONDS))
    } finally s.close()
  }

  @Test
  def testCreatePodFailed(): Unit = {
    val s = createFailedPod()
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      intercept[IllegalArgumentException] {
        Await.result(
          client.containerCreator
            .name("is-land.hsinchu")
            .imageName("hello world")
            .hostname("test1")
            .nodeName("node1")
            .create(),
          Duration(30, TimeUnit.SECONDS)
        )
      }.getMessage shouldBe "host name error"
    } finally s.close()
  }

  @Test
  def testNodes(): Unit = {
    val s = nodes()
    try {
      val client                    = K8SClient.builder.serverURL(s.url).build()
      val nodes: Seq[K8SNodeReport] = Await.result(client.nodes(), Duration(5, TimeUnit.SECONDS))
      nodes.size shouldBe 3
      nodes(0).nodeName shouldBe "ohara-jenkins-it-00"
      nodes(1).nodeName shouldBe "ohara-jenkins-it-01"
      nodes(2).nodeName shouldBe "ohara-jenkins-it-02"
    } finally s.close()
  }

  @Test
  def testNotSettingMetricsURL(): Unit = {
    val s = nodes()
    try {
      val client = K8SClient.builder.serverURL(s.url).build()
      val result = Await.result(client.resources(), Duration(5, TimeUnit.SECONDS))
      result.size shouldBe 0
      result.isEmpty shouldBe true
    } finally s.close()
  }

  @Test
  def testK8SMetricsResource(): Unit = {
    val s = resources()
    try {
      val k8sClient          = K8SClient.builder.serverURL(s.url).metricsServerURL(s.url).build()
      val resource           = Await.result(k8sClient.resources(), Duration(5, TimeUnit.SECONDS))
      val nodes: Seq[String] = resource.keys.toSeq
      nodes(0) shouldBe "ohara-jenkins-it-00"

      val node1Resource: Seq[Resource] = resource
        .filter { case (key, _) => key == nodes.head }
        .flatMap { case (_, value) => value }
        .toSet
        .toSeq
      node1Resource(0).name shouldBe "CPU"
      node1Resource(0).unit shouldBe "cores"
      node1Resource(0).value shouldBe 8.0
      node1Resource(0).used.get > 0.04 shouldBe true

      node1Resource(1).name shouldBe "Memory"
      node1Resource(1).unit shouldBe "GB"
      node1Resource(1).used.get > 0.08 shouldBe true
    } finally s.close()
  }

  @Test
  def testEmptyMetricsResource(): Unit = {
    val s = emptyResources()
    try {
      val k8sClient          = K8SClient.builder.serverURL(s.url).metricsServerURL(s.url).build()
      val resource           = Await.result(k8sClient.resources(), Duration(5, TimeUnit.SECONDS))
      val nodes: Seq[String] = resource.keys.toSeq
      nodes(0) shouldBe "ohara-jenkins-it-00"

      val node1Resource: Seq[Resource] = resource
        .filter {
          case (key, _) => key == nodes.head
        }
        .flatMap { case (_, value) => value }
        .toSet
        .toSeq
      node1Resource.size shouldBe 0
    } finally s.close()
  }

  @Test
  def testCPUUsedCalc(): Unit = {
    K8SClient.cpuUsedCalc("160527031n", 8) shouldBe 0.020065878875
    K8SClient.cpuUsedCalc("149744u", 8) shouldBe 0.018718
    K8SClient.cpuUsedCalc("338m", 8) shouldBe 0.04225
  }

  @Test
  def testCPUUsedCalcError(): Unit = {
    an[IllegalArgumentException] should be thrownBy {
      K8SClient.cpuUsedCalc("338000a", 8)
    }
  }

  @Test
  def testMemoryUsedCalc(): Unit = {
    // 31457280 KB = 30 GB
    K8SClient.memoryUsedCalc("3706620Ki", 31457280) > 0.11 shouldBe true
    K8SClient.memoryUsedCalc("3619Mi", 31457280) > 0.11 shouldBe true
    K8SClient.memoryUsedCalc("16Gi", 31457280) > 0.53 shouldBe true
    K8SClient.memoryUsedCalc("0.0156Ti", 31457280) > 0.53 shouldBe true
    K8SClient.memoryUsedCalc("0.000015259Pi", 31457280) > 0.53 shouldBe true
    K8SClient.memoryUsedCalc("0.000000015Ei", 31457280) > 0.53 shouldBe true
  }

  @Test
  def testMemoryUsedCalcError(): Unit = {
    an[IllegalArgumentException] should be thrownBy {
      K8SClient.memoryUsedCalc("338000Fi", 8)
    }
  }

  private[this] def emptyResources(): SimpleServer = {
    val nodeMetrics: String = s"""
                                 |{
                                 |   "items":[]
                                 |}
       """.stripMargin
    val nodes: String       = s"""
                           |{
                           |   "items":[
                           |      {
                           |         "metadata":{
                           |            "name":"ohara-jenkins-it-00"
                           |         },
                           |         "status":{
                           |            "addresses":[
                           |               {
                           |                  "type":"InternalIP",
                           |                  "address":"10.2.0.8"
                           |               }
                           |            ],
                           |            "images":[],
                           |            "conditions":[],
                           |            "allocatable":{
                           |               "cpu":"8",
                           |               "memory":"30612932Ki"
                           |            }
                           |         }
                           |      }
                           |   ]
                           |}
       """.stripMargin

    toServer {
      path("metrics.k8s.io" / "v1beta1" / "nodes") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, nodeMetrics)))
        }
      } ~
        path("nodes") {
          get {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, nodes)))
          }
        }
    }
  }

  private[this] def resources(): SimpleServer = {
    val nodeMetrics: String = s"""
         |{
         |   "items":[
         |      {
         |         "metadata":{
         |            "name":"ohara-jenkins-it-00"
         |         },
         |         "usage":{
         |            "cpu":"332859630n",
         |            "memory":"2512480Ki"
         |         }
         |      }
         |   ]
         |}
       """.stripMargin
    val nodes: String       = s"""
         |{
         |   "items":[
         |      {
         |         "metadata":{
         |            "name":"ohara-jenkins-it-00"
         |         },
         |         "status":{
         |            "addresses":[
         |               {
         |                  "type":"InternalIP",
         |                  "address":"10.2.0.8"
         |               }
         |            ],
         |            "images":[],
         |            "conditions":[],
         |            "allocatable":{
         |               "cpu":"8",
         |               "memory":"30612932Ki"
         |            }
         |         }
         |      }
         |   ]
         |}
       """.stripMargin

    toServer {
      path("metrics.k8s.io" / "v1beta1" / "nodes") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, nodeMetrics)))
        }
      } ~
        path("nodes") {
          get {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, nodes)))
          }
        }
    }
  }

  private[this] def nodes(): SimpleServer = {
    val response: String = s"""
         |{"items": [
         |  {
         |    "metadata": {
         |      "name": "ohara-jenkins-it-00"
         |    },
         |    "status": {
         |      "addresses": [
         |        {
         |          "type": "InternalIP",
         |          "address": "10.2.0.8"
         |         }
         |      ],
         |      "images": [],
         |      "conditions": []
         |    }
         |  },
         |  {
         |    "metadata": {
         |      "name": "ohara-jenkins-it-01"
         |    },
         |    "status": {
         |      "addresses": [
         |        {
         |          "type": "InternalIP",
         |          "address": "10.2.0.8"
         |        }
         |      ],
         |      "images": [],
         |      "conditions": []
         |    }
         |  },
         |  {
         |    "metadata": {
         |      "name": "ohara-jenkins-it-02"
         |    },
         |    "status": {
         |      "addresses": [
         |        {
         |          "type": "InternalIP",
         |          "address": "10.2.0.8"
         |         }
         |      ],
         |      "images": [],
         |      "conditions": []
         |    }
         |  }
         |]}
       """.stripMargin

    toServer {
      path("nodes") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, response)))
        }
      }
    }
  }

  private[this] def imagePolicyURL(
    nodeName: String,
    podName: String,
    expectImagePullPolicy: ImagePullPolicy
  ): SimpleServer = {
    val nodesResponse = s"""
                           |{"items": [
                           |    {
                           |      "metadata": {
                           |        "name": "$nodeName"
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
                           |}
                """.stripMargin

    val createPodResult = s"""
                             |{
                             |  "metadata": {
                             |    "name": "$podName",
                             |    "uid": "aaaaaaaaaaaa",
                             |    "creationTimestamp": "2019-05-13 00:00:00"
                             |  },
                             |  "status": {
                             |    "phase": "true"
                             |  }
                             |}
                """.stripMargin
    val volumeInfo =
      s"""
         |{
         |  "items": []
         |}
       """.stripMargin
    // test json serialization
    val nodeInfo: NodeInfo = NODEINFO_FORMAT.read(nodesResponse.parseJson)
    nodeInfo.items.head.metadata.name shouldBe "ohara-it-02"
    nodeInfo.items.head.status.images.size shouldBe 1

    // test communication
    toServer {
      path("persistentvolumes") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, volumeInfo)))
        }
      } ~
        path("nodes") {
          get {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, nodesResponse)))
          }
        } ~
        path("namespaces" / namespace / "pods") {
          post {
            entity(as[Pod]) { createPod =>
              createPod.spec.get.containers.head.imagePullPolicy shouldBe Some(expectImagePullPolicy)
              complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, createPodResult)))
            }
          }
        }
    }
  }

  private[this] def log(podName: String): SimpleServer = {
    val logMessage = "start pods ......."
    toServer {
      path("namespaces" / namespace / "pods") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, podsInfo)))
        }
      } ~ path("namespaces" / namespace / "pods" / podName / "log") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, logMessage)))
        }
      }
    }
  }

  private[this] def createFailedPod(): SimpleServer = {
    val nodesResponse = s"""
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
                           |}
                """.stripMargin

    val resultMessage = s"""
                           |{
                           |  "message": "host name error"
                           |}
       """.stripMargin
    val volumeInfo =
      s"""
         |{
         |  "items": []
         |}
       """.stripMargin
    // test communication
    toServer {
      path("persistentvolumes") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, volumeInfo)))
        }
      } ~
        path("nodes") {
          get {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, nodesResponse)))
          }
        } ~
        path("namespaces" / namespace / "pods") {
          post {
            entity(as[Pod]) { _ =>
              complete(
                HttpResponse(
                  status = StatusCodes.BadRequest,
                  entity = HttpEntity(ContentTypes.`application/json`, resultMessage)
                )
              )
            }
          }
        }
    }
  }

  private[this] def forceRemovePodURL(podName: String): SimpleServer = {
    // test communication
    toServer {
      path("namespaces" / namespace / "pods") {
        get {
          complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, podsInfo)))
        }
      } ~
        path("namespaces" / namespace / "pods" / podName) {
          delete {
            complete(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, podsInfo)))
          }
        }
    }
  }

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
}
