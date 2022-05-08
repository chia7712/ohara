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

package oharastream.ohara.it

import java.net.URL
import java.util.Objects
import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.docker.DockerClient
import oharastream.ohara.agent.k8s.K8SClient
import oharastream.ohara.agent.{DataCollie, RemoteFolderHandler}
import oharastream.ohara.client.configurator.NodeApi.{Node, State}
import oharastream.ohara.client.configurator.{
  BrokerApi,
  ContainerApi,
  FileInfoApi,
  InspectApi,
  LogApi,
  NodeApi,
  ShabondiApi,
  StreamApi,
  TopicApi,
  VolumeApi,
  WorkerApi,
  ZookeeperApi
}
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable, VersionUtils}
import oharastream.ohara.it.ContainerPlatform.ResourceRef

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

trait ContainerPlatform {
  /**
    * setup all runtime services. The return object must be released after completing test case. Normally, it should be
    * called by after phase
    * @return a object amassing runtime object. For example, configurator service and container client
    */
  def setup(): ResourceRef

  /**
    * setup only container client. If your IT requires only container client, please use this method as it is cheaper
    * then setup().
    * @return container client
    */
  def setupContainerClient(): ContainerClient

  /**
    * @return the node names exists on Configurator
    */
  def nodeNames: Set[String]
}

object ContainerPlatform {
  trait ResourceRef extends Releasable {
    def configuratorHostname: String
    def configuratorPort: Int
    def containerClient: ContainerClient

    /**
      * create and register a object key. Those generated keys get closed after releasing ResourceRef
      * @return object key
      */
    def generateObjectKey: ObjectKey

    /**
      * @return the node names exists on Configurator
      */
    def nodeNames: Set[String] = nodes.map(_.hostname).toSet

    def nodes: Seq[Node]
    //----------------[helpers]----------------//
    def zookeeperApi: ZookeeperApi.Access =
      ZookeeperApi.access.hostname(configuratorHostname).port(configuratorPort)
    def brokerApi: BrokerApi.Access = BrokerApi.access.hostname(configuratorHostname).port(configuratorPort)
    def workerApi: WorkerApi.Access = WorkerApi.access.hostname(configuratorHostname).port(configuratorPort)
    def streamApi: StreamApi.Access = StreamApi.access.hostname(configuratorHostname).port(configuratorPort)
    def volumeApi: VolumeApi.Access = VolumeApi.access.hostname(configuratorHostname).port(configuratorPort)

    def containerApi: ContainerApi.Access =
      ContainerApi.access.hostname(configuratorHostname).port(configuratorPort)
    def topicApi: TopicApi.Access = TopicApi.access.hostname(configuratorHostname).port(configuratorPort)
    def fileApi: FileInfoApi.Access =
      FileInfoApi.access.hostname(configuratorHostname).port(configuratorPort)
    def nodeApi: NodeApi.Access =
      NodeApi.access.hostname(configuratorHostname).port(configuratorPort)
    def logApi: LogApi.Access =
      LogApi.access.hostname(configuratorHostname).port(configuratorPort)
    def inspectApi: InspectApi.Access =
      InspectApi.access.hostname(configuratorHostname).port(configuratorPort)

    def shabondiApi: ShabondiApi.Access = ShabondiApi.access.hostname(configuratorHostname).port(configuratorPort)
  }

  private[ContainerPlatform] def result[T](f: Future[T]): T = Await.result(f, Duration(120, TimeUnit.SECONDS))

  private val K8S_COORDINATOR_URL_KEY: String    = "ohara.it.k8s"
  private val K8S_METRICS_SERVER_URL_KEY: String = "ohara.it.k8s.metrics.server"
  private val K8S_NAMESPACE_KEY: String          = "ohara.it.k8s.namespace"

  /**
    * form: user:password@hostname:port.
    * NOTED: this key need to be matched with another key value in ohara-it/build.gradle
    */
  val DOCKER_NODES_KEY = "ohara.it.docker"
  private[this] def _k8sMode: Option[ContainerPlatform] =
    Seq(
      sys.env.get(ContainerPlatform.K8S_COORDINATOR_URL_KEY),
      // k8s mode needs user and password passed by docker arguments
      sys.env.get(ContainerPlatform.DOCKER_NODES_KEY)
    ).flatten match {
      case Seq(coordinatorUrl, plainNodes) =>
        val nodes = parserNode(plainNodes)
        def createClient(): K8SClient =
          K8SClient.builder
            .serverURL(coordinatorUrl)
            .namespace(sys.env.getOrElse(K8S_NAMESPACE_KEY, "default"))
            .metricsServerURL(sys.env.get(ContainerPlatform.K8S_METRICS_SERVER_URL_KEY).orNull)
            .remoteFolderHandler(RemoteFolderHandler(DataCollie(nodes)))
            .build()
        val containerClient = createClient()
        try Some(
          ContainerPlatform.builder
          // the coordinator node is NOT able to run pods by default
            .coordinatorName(new URL(coordinatorUrl).getHost)
            .mode("K8S")
            .nodes(nodes)
            .clientCreator(() => createClient())
            .arguments(
              Seq(
                "--k8s",
                coordinatorUrl
              ) ++ containerClient.metricsUrl.map(s => Seq("--k8s-metrics-server", s)).getOrElse(Seq.empty)
            )
            .build
        )
        finally Releasable.close(containerClient)
      case _ =>
        None
    }

  /**
    * @return k8s platform information. Or skip test
    */
  def k8sMode: ContainerPlatform = _k8sMode.get

  /**
    * @return docker nodes information passed by env
    */
  def dockerNodes: Option[Seq[Node]] =
    sys.env
      .get(ContainerPlatform.DOCKER_NODES_KEY)
      .map(parserNode)

  private[this] def parserNode(plainNodes: String): Seq[Node] = {
    def parse(nodeInfo: String): Node = {
      val user     = nodeInfo.split(":").head
      val password = nodeInfo.split("@").head.split(":").last
      val hostname = nodeInfo.split("@").last.split(":").head
      val port     = nodeInfo.split("@").last.split(":").last.toInt
      Node(
        hostname = hostname,
        port = port,
        user = user,
        password = password,
        services = Seq.empty,
        state = State.AVAILABLE,
        error = None,
        lastModified = CommonUtils.current(),
        resources = Seq.empty,
        tags = Map.empty
      )
    }
    plainNodes.split(",").map(parse).toSeq
  }

  private[this] def _dockerMode: Option[ContainerPlatform] =
    dockerNodes
      .map(
        nodes =>
          ContainerPlatform.builder
            .mode("DOCKER")
            .nodes(nodes)
            .clientCreator(() => DockerClient(DataCollie(nodes)))
            .build
      )

  /**
    * @return docker platform information. Or skip test
    */
  def dockerMode: ContainerPlatform = _dockerMode.get

  private[this] val ERROR_MESSAGE = s"please set ${ContainerPlatform.K8S_COORDINATOR_URL_KEY} and ${ContainerPlatform.K8S_METRICS_SERVER_URL_KEY}" +
    s"to run the IT on k8s mode; Or set ${ContainerPlatform.DOCKER_NODES_KEY} to run IT on docker mode"

  /**
    * The order of lookup is shown below.
    * 1) k8s setting - PlatformModeInfo.K8S_COORDINATOR_URL_KEY and PlatformModeInfo.K8S_METRICS_SERVER_URL
    * 2) docker setting - PlatformModeInfo.DOCKER_NODES_KEY
    * @return one of k8s or docker. If they are nonexistent, a AssumptionViolatedException is thrown
    */
  def default: ContainerPlatform = _k8sMode.orElse(_dockerMode).getOrElse(throw new RuntimeException(ERROR_MESSAGE))

  /**
    * @return k8s + docker. Or empty collection
    */
  def all: Seq[ContainerPlatform] = (_dockerMode ++ _k8sMode).toSeq

  def builder = new Builder

  private[ContainerPlatform] class Builder extends oharastream.ohara.common.pattern.Builder[ContainerPlatform] {
    private[this] var mode: String                         = _
    private[this] var nodes: Seq[Node]                     = Seq.empty
    private[this] var arguments: Seq[String]               = Seq.empty
    private[this] var coordinatorName: Option[String]      = None
    private[this] var clientCreator: () => ContainerClient = _

    def mode(mode: String): Builder = {
      this.mode = CommonUtils.requireNonEmpty(mode)
      this
    }

    def nodes(nodes: Seq[Node]): Builder = {
      this.nodes = CommonUtils.requireNonEmpty(nodes.asJava).asScala.toSeq
      this
    }

    def clientCreator(clientCreator: () => ContainerClient): Builder = {
      this.clientCreator = Objects.requireNonNull(clientCreator)
      this
    }

    def arguments(arguments: Seq[String]): Builder = {
      this.arguments = Objects.requireNonNull(arguments)
      this
    }

    /**
      * the coordinator name is NOT running any service later.
      * @param coordinatorName coordinator hostname
      * @return this builder
      */
    def coordinatorName(coordinatorName: String): Builder = {
      this.coordinatorName = Some(coordinatorName)
      this
    }

    private[this] def followerNodes: Seq[Node] = nodes.filterNot(n => coordinatorName.contains(n.hostname))

    private[this] def createConfigurator(containerClient: ContainerClient): (String, String, Int) =
      try {
        val configuratorHostname = {
          val images = result(containerClient.imageNames())
          images
            .filter(_._2.contains(s"oharastream/configurator:${VersionUtils.VERSION}"))
            .filterNot(e => coordinatorName.contains(e._1))
            .keys
            .headOption
            .getOrElse(
              throw new RuntimeException(
                s"failed to find oharastream/configurator:${VersionUtils.VERSION} from nodes:${images.keySet.mkString(",")}"
              )
            )
        }

        val configuratorName = s"configurator-${CommonUtils.randomString(10)}"
        val configuratorPort = CommonUtils.availablePort()
        result(
          containerClient.containerCreator
            .nodeName(configuratorHostname)
            .imageName(s"oharastream/configurator:${VersionUtils.VERSION}")
            .portMappings(Map(configuratorPort -> configuratorPort))
            .arguments(
              Seq(
                "--hostname",
                configuratorHostname,
                "--port",
                configuratorPort.toString
              ) ++ Objects.requireNonNull(arguments)
            )
            // add the routes manually since not all envs have deployed the DNS.
            .routes(followerNodes.map(node => node.hostname -> CommonUtils.address(node.hostname)).toMap)
            .name(configuratorName)
            .create()
        )
        try {
          val nodeApi = NodeApi.access.hostname(configuratorHostname).port(configuratorPort)
          // wait configurator to run and update the nodes to configurator
          CommonUtils.await(
            () => {
              val existentNodes = try result(nodeApi.list())
              catch {
                case _: Throwable => Seq.empty
              }
              followerNodes
                .filterNot(node => existentNodes.exists(_.hostname == node.hostname))
                .foreach(node => Releasable.close(() => result(nodeApi.request.node(node).create())))
              existentNodes.size == followerNodes.size
            },
            java.time.Duration.ofSeconds(60)
          )
          (configuratorName, configuratorHostname, configuratorPort)
        } catch {
          case e: Throwable =>
            Releasable.close(() => result(containerClient.forceRemove(configuratorName)))
            Releasable.close(containerClient)
            throw e
        }
      } finally Releasable.close(containerClient)

    override def build: ContainerPlatform = {
      Objects.requireNonNull(arguments)
      new ContainerPlatform {
        override def setup(): ResourceRef = {
          val (configuratorName, hostname, port) = createConfigurator(clientCreator())
          new ResourceRef {
            private[this] val serviceKeyHolder        = ServiceKeyHolder(clientCreator())
            override def generateObjectKey: ObjectKey = serviceKeyHolder.generateObjectKey()
            override def configuratorHostname: String = hostname
            override def configuratorPort: Int        = port
            override def close(): Unit = {
              println(s"[----------------------------configurator:$configuratorName----------------------------]")
              Releasable.close(() => println(result(containerClient.log(configuratorName))))
              println("[------------------------------------------------------------------------------------]")
              Releasable.close(() => result(containerClient.forceRemove(configuratorName)))
              Releasable.close(containerClient)
              Releasable.close(serviceKeyHolder)
            }
            override lazy val containerClient: ContainerClient = clientCreator()

            override def nodes: Seq[Node] =
              CommonUtils.requireNonEmpty(Builder.this.followerNodes.asJava).asScala.toSeq
          }
        }

        override def setupContainerClient(): ContainerClient = clientCreator()

        override val nodeNames: Set[String] =
          CommonUtils.requireNonEmpty(Builder.this.followerNodes.asJava).asScala.map(_.hostname).toSet

        override val toString: String = CommonUtils.requireNonEmpty(mode)
      }
    }
  }
}
