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

import java.util.Objects

import oharastream.ohara.agent.container.ContainerClient.VolumeCreator
import oharastream.ohara.agent.container.{ContainerClient, ContainerName, ContainerVolume}
import oharastream.ohara.agent.k8s.K8SClient.ContainerCreator
import oharastream.ohara.agent.k8s.K8SJson._
import oharastream.ohara.client.HttpExecutor
import oharastream.ohara.client.configurator.ContainerApi.{ContainerInfo, PortMapping}
import oharastream.ohara.client.configurator.NodeApi.Resource
import oharastream.ohara.client.configurator.VolumeApi.Volume
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.pattern.Builder
import oharastream.ohara.common.util.CommonUtils

import scala.concurrent.{ExecutionContext, Future}

case class K8SStatusInfo(isHealth: Boolean, message: String)
case class K8SNodeReport(nodeName: String, imageNames: Seq[String])
case class Report(nodeName: String, isK8SNode: Boolean, statusInfo: Option[K8SStatusInfo])

trait K8SClient extends ContainerClient {
  override def containerCreator: ContainerCreator
  def nodeNameIPInfo()(implicit executionContext: ExecutionContext): Future[Seq[HostAliases]]
  def checkNode(nodeName: String)(implicit executionContext: ExecutionContext): Future[Report]
  def nodes()(implicit executionContext: ExecutionContext): Future[Seq[K8SNodeReport]]
  def coordinatorUrl: String
  def metricsUrl: Option[String]
}

object K8SClient {
  /**
    * this is a specific label to ohara docker. It is useful in filtering out what we created.
    */
  private[this] val LABEL_KEY   = "createdByOhara"
  private[this] val LABEL_VALUE = "k8s"
  @VisibleForTesting
  private[k8s] val NAMESPACE_DEFAULT_VALUE = "default"

  def builder: K8SClientBuilder = new K8SClientBuilder()

  private[K8SClient] class K8SClientBuilder extends Builder[K8SClient] {
    private[this] var k8sApiServerURL: String        = _
    private[this] var k8sMetricsApiServerURL: String = _
    private[this] var k8sNamespace: String           = NAMESPACE_DEFAULT_VALUE

    /**
      * You must set the Kubernetes API server url, default value is null
      * @param k8sApiServerURL Kubernetes API Server URL
      * @return K8SClientBuilder object
      */
    def apiServerURL(k8sApiServerURL: String): K8SClientBuilder = {
      this.k8sApiServerURL = CommonUtils.requireNonEmpty(k8sApiServerURL)
      this
    }

    /**
      * You can set other namespace name for Kubrnetes, default value is default
      * @param k8sNamespace Kubenretes namespace name
      * @return K8SClientBuilder object
      */
    @Optional("default value is default")
    def namespace(k8sNamespace: String): K8SClientBuilder = {
      if (CommonUtils.isEmpty(k8sNamespace)) this.k8sNamespace = NAMESPACE_DEFAULT_VALUE
      else this.k8sNamespace = k8sNamespace
      this
    }

    /**
      * Set K8S metrics server URL
      * @param k8sMetricsApiServerURL for set Kubernetes metrics api server url, default value is null
      * @return K8SClientBuilder object
      */
    @Optional("default value is null")
    def metricsApiServerURL(k8sMetricsApiServerURL: String): K8SClientBuilder = {
      this.k8sMetricsApiServerURL = k8sMetricsApiServerURL
      this
    }

    /**
      * Return the K8SClient object to operate Kubernetes api server
      * @return K8SClient object
      */
    def build(): K8SClient = {
      CommonUtils.requireNonEmpty(k8sApiServerURL)
      CommonUtils.requireNonEmpty(k8sNamespace)

      new K8SClient() {
        override def coordinatorUrl: String     = k8sApiServerURL
        override def metricsUrl: Option[String] = Option(k8sMetricsApiServerURL)
        override def containers()(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]] =
          httpExecutor
            .get[PodList, ErrorResponse](s"$k8sApiServerURL/namespaces/$k8sNamespace/pods")
            .map(
              podList =>
                podList.items
                // filter out the k8s containers which are NOT created by ohara k8s
                  .filter(_.metadata.labels.exists(_.get(LABEL_KEY).exists(_ == LABEL_VALUE)))
                  .map(pod => {
                    val spec: PodSpec = pod.spec
                      .getOrElse(throw new RuntimeException(s"the container doesn't have spec : ${pod.metadata.name}"))
                    val containerInfo: Container = spec.containers.head
                    val phase                    = pod.status.map(_.phase).getOrElse("Unknown")
                    val hostIP                   = pod.status.fold("Unknown")(_.hostIP.getOrElse("Unknown"))
                    ContainerInfo(
                      nodeName = spec.nodeName.getOrElse("Unknown"),
                      id = pod.metadata.uid.getOrElse("Unknown"),
                      imageName = containerInfo.image,
                      state = K8sContainerState.all
                        .find(s => phase.toLowerCase().contains(s.name.toLowerCase))
                        .getOrElse(K8sContainerState.UNKNOWN)
                        .name,
                      kind = K8S_KIND_NAME,
                      size = -1,
                      name = pod.metadata.name,
                      portMappings = containerInfo.ports
                        .getOrElse(Seq.empty)
                        .map(x => PortMapping(hostIP, x.hostPort, x.containerPort)),
                      environments = containerInfo.env.getOrElse(Seq()).map(x => x.name -> x.value.getOrElse("")).toMap,
                      hostname = spec.hostname
                    )
                  })
            )

        override def imageNames()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[String]]] =
          nodes()
            .map(_.map { node =>
              node.nodeName -> node.imageNames
            }.toMap)

        override def checkNode(nodeName: String)(implicit executionContext: ExecutionContext): Future[Report] =
          httpExecutor.get[NodeInfo, ErrorResponse](s"$k8sApiServerURL/nodes").map { r =>
            val filterNode: Seq[NodeItems]        = r.items.filter(x => x.metadata.name.equals(nodeName))
            val isK8SNode: Boolean                = filterNode.size == 1
            var statusInfo: Option[K8SStatusInfo] = None
            if (isK8SNode)
              statusInfo = Some(
                filterNode
                  .flatMap(x => {
                    x.status.conditions.filter(y => y.conditionType.equals("Ready"))
                  }.map(x => {
                    if (x.status.equals("True")) K8SStatusInfo(true, x.message)
                    else K8SStatusInfo(false, x.message)
                  }))
                  .head
              )
            Report(nodeName, isK8SNode, statusInfo)
          }

        override def forceRemove(name: String)(implicit executionContext: ExecutionContext): Future[Unit] =
          removePod(name, true)

        override def remove(name: String)(implicit executionContext: ExecutionContext): Future[Unit] =
          removePod(name, false)

        override def log(name: String, sinceSeconds: Option[Long])(
          implicit executionContext: ExecutionContext
        ): Future[Map[ContainerName, String]] =
          containerNames(name)
            .flatMap(
              Future.traverse(_)(
                containerName =>
                  httpExecutor
                    .getOnlyMessage(
                      sinceSeconds
                        .map(
                          seconds => s"$k8sApiServerURL/namespaces/$k8sNamespace/pods/$name/log?sinceSeconds=$seconds"
                        )
                        .getOrElse(s"$k8sApiServerURL/namespaces/$k8sNamespace/pods/$name/log")
                    )
                    .map(
                      msg =>
                        if (msg.contains("ERROR:")) throw new IllegalArgumentException(msg) else containerName -> msg
                    )
              )
            )
            .map(_.toMap)

        override def nodeNameIPInfo()(implicit executionContext: ExecutionContext): Future[Seq[HostAliases]] =
          httpExecutor
            .get[NodeInfo, ErrorResponse](s"$k8sApiServerURL/nodes")
            .map(
              nodeInfo =>
                nodeInfo.items.map(item => {
                  val internalIP: String =
                    item.status.addresses.filter(node => node.nodeType.equals("InternalIP")).head.nodeAddress
                  val hostName: String =
                    item.status.addresses.filter(node => node.nodeType.equals("Hostname")).head.nodeAddress
                  HostAliases(internalIP, Seq(hostName))
                })
            )

        override def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]] = {
          if (k8sMetricsApiServerURL == null) Future.successful(Map.empty)
          else {
            // Get K8S metrics
            val nodeResourceUsage: Future[Map[String, K8SJson.MetricsUsage]] = httpExecutor
              .get[Metrics, ErrorResponse](s"$k8sMetricsApiServerURL/metrics.k8s.io/v1beta1/nodes")
              .map(metrics => {
                metrics.items
                  .flatMap(nodeMetricsInfo => {
                    Seq(
                      nodeMetricsInfo.metadata.name ->
                        MetricsUsage(nodeMetricsInfo.usage.cpu, nodeMetricsInfo.usage.memory)
                    )
                  })
                  .toMap
              })

            // Get K8S Node info
            httpExecutor
              .get[NodeInfo, ErrorResponse](s"$k8sApiServerURL/nodes")
              .map(
                nodeInfo =>
                  nodeInfo.items
                    .map { item =>
                      val allocatable =
                        item.status.allocatable.getOrElse(Allocatable(None, None))
                      (item.metadata.name, allocatable.cpu, allocatable.memory)
                    }
                    .map { nodeResource =>
                      nodeResourceUsage.map {
                        resourceUsage =>
                          val nodeName: String    = nodeResource._1
                          val cpuValueCore: Int   = nodeResource._2.getOrElse("0").toInt
                          val memoryValueKB: Long = nodeResource._3.getOrElse("0").replace("Ki", "").toLong
                          if (resourceUsage.contains(nodeName)) {
                            // List all resource unit for Kubernetes metrics server, Please refer the source code:
                            // https://github.com/kubernetes/apimachinery/blob/ed135c5b96450fd24e5e981c708114fbbd950697/pkg/api/resource/suffix.go
                            val cpuUsed: Option[Double] = Option(cpuUsedCalc(resourceUsage(nodeName).cpu, cpuValueCore))
                            val memoryUsed: Option[Double] =
                              Option(memoryUsedCalc(resourceUsage(nodeName).memory, memoryValueKB))
                            nodeName -> Seq(
                              Resource.cpu(cpuValueCore, cpuUsed),
                              Resource.memory(memoryValueKB * 1024, memoryUsed)
                            )
                          } else nodeName -> Seq.empty
                      }
                    }
              )
              .flatMap(Future.sequence(_))
              .map(_.toMap)
          }
        }

        override def nodes()(implicit executionContext: ExecutionContext): Future[Seq[K8SNodeReport]] = {
          httpExecutor
            .get[NodeInfo, ErrorResponse](s"$k8sApiServerURL/nodes")
            .map(
              nodeInfo =>
                nodeInfo.items.map(
                  item => K8SNodeReport(nodeName = item.metadata.name, imageNames = item.status.images.flatMap(_.names))
                )
            )
        }

        override def containerCreator: ContainerCreator =
          new ContainerCreator() {
            private[this] var imagePullPolicy: ImagePullPolicy = ImagePullPolicy.IFNOTPRESENT
            private[this] var restartPolicy: RestartPolicy     = RestartPolicy.Never
            private[this] val domainName: String               = "default"
            private[this] val labelName: String                = "ohara"

            @Optional
            override def pullImagePolicy(imagePullPolicy: ImagePullPolicy): ContainerCreator = {
              this.imagePullPolicy = Objects.requireNonNull(imagePullPolicy, "pullImagePolicy should not be null")
              this
            }

            @Optional("default is Never")
            override def restartPolicy(restartPolicy: RestartPolicy): ContainerCreator = {
              this.restartPolicy = Objects.requireNonNull(restartPolicy, "restartPolicy should not be null")
              this
            }

            override protected def doCreate(
              nodeName: String,
              hostname: String,
              imageName: String,
              volumeMaps: Map[Volume, String],
              name: String,
              command: Option[String],
              arguments: Seq[String],
              ports: Map[Int, Int],
              envs: Map[String, String],
              routes: Map[String, String],
              executionContext: ExecutionContext
            ): Future[Unit] = {
              // required fields
              CommonUtils.requireNonEmpty(domainName)
              CommonUtils.requireNonEmpty(labelName)
              implicit val pool: ExecutionContext = executionContext
              nodeNameIPInfo()
                .map { ipInfo =>
                  PodSpec(
                    nodeSelector = Some(NodeSelector(nodeName)),
                    hostname = hostname, //hostname is container name
                    subdomain = Some(domainName),
                    hostAliases = Some(ipInfo ++ routes.map { case (host, ip) => HostAliases(ip, Seq(host)) }),
                    containers = Seq(
                      Container(
                        name = labelName,
                        image = imageName,
                        volumeMounts =
                          if (volumeMaps.isEmpty) None
                          else
                            Some(volumeMaps.map(v => VolumeMount(v._1.name, v._2)).toSeq),
                        env = if (envs.isEmpty) None else Some(envs.map(x => EnvVar(x._1, Some(x._2))).toSeq),
                        ports = if (ports.isEmpty) None else Some(ports.map(x => ContainerPort(x._1, x._2)).toSeq),
                        imagePullPolicy = Some(imagePullPolicy),
                        command = command.map(Seq(_)),
                        args = if (arguments.isEmpty) None else Some(arguments)
                      )
                    ),
                    restartPolicy = Some(restartPolicy),
                    nodeName = None,
                    volumes =
                      if (volumeMaps.isEmpty) None
                      else
                        Some(
                          volumeMaps
                            .map(v => K8SVolume(v._1.key.toPlain, Some(MountPersistentVolumeClaim(v._1.key.toPlain))))
                            .toSeq
                        )
                  )
                }
                .flatMap(
                  podSpec =>
                    httpExecutor
                      .post[Pod, Pod, ErrorResponse](
                        s"$k8sApiServerURL/namespaces/$k8sNamespace/pods",
                        Pod(Metadata(None, name, Some(Map(LABEL_KEY -> LABEL_VALUE)), None), Some(podSpec), None)
                      )
                )
                .map(_ => ())
            }
          }

        private[this] def removePod(name: String, isForce: Boolean)(
          implicit executionContext: ExecutionContext
        ): Future[Unit] = {
          val isForceRemovePod: String = if (isForce) "?gracePeriodSeconds=0" else ""
          containers(name)
            .flatMap(
              Future.traverse(_)(
                container =>
                  httpExecutor
                    .delete[ErrorResponse](
                      s"$k8sApiServerURL/namespaces/$k8sNamespace/pods/${container.name}${isForceRemovePod}"
                    )
              )
            )
            .map(_ => ())
        }

        override def close(): Unit = {
          // do nothing
        }

        override def volumeCreator: VolumeCreator =
          (nodeName: String, volumeName: String, path: String, executionContext: ExecutionContext) => {
            implicit val pool: ExecutionContext = executionContext
            httpExecutor
              .post[PersistentVolume, PersistentVolume, ErrorResponse](
                s"$k8sApiServerURL/persistentvolumes",
                PersistentVolume(
                  PVMetadata(volumeName),
                  PVSpec(
                    capacity = PVCapacity("500Gi"),
                    accessModes = Seq("ReadWriteOnce"),
                    persistentVolumeReclaimPolicy = "Retain",
                    storageClassName = volumeName,
                    hostPath = PVHostPath(path, "DirectoryOrCreate"),
                    nodeAffinity = PVNodeAffinity(
                      PVRequired(
                        Seq(
                          PVNodeSelectorTerm(
                            Seq(PVMatchExpression("kubernetes.io/hostname", "In", Seq(nodeName)))
                          )
                        )
                      )
                    )
                  )
                )
              )
              .flatMap { _ =>
                httpExecutor
                  .post[PersistentVolumeClaim, PersistentVolumeClaim, ErrorResponse](
                    s"$k8sApiServerURL/namespaces/${k8sNamespace}/persistentvolumeclaims",
                    PersistentVolumeClaim(
                      PVCMetadata(volumeName),
                      PVCSpec(
                        storageClassName = volumeName,
                        accessModes = Seq("ReadWriteOnce"),
                        resources = PVCResources(PVCRequests("500Gi"))
                      )
                    )
                  )
              }
              .map(_ => ())
          }

        override def removeVolumes(name: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
          httpExecutor
            .delete[ErrorResponse](
              s"$k8sApiServerURL/namespaces/$k8sNamespace/persistentvolumeclaims/${name}?gracePeriodSeconds=0"
            )
            .flatMap { _ =>
              httpExecutor
                .delete[ErrorResponse](
                  s"$k8sApiServerURL/persistentvolumes/${name}?gracePeriodSeconds=0"
                )
            }
            .map(_ => ())
        }

        override def volumes()(implicit executionContext: ExecutionContext): Future[Seq[ContainerVolume]] = {
          httpExecutor
            .get[PersistentVolumeInfo, ErrorResponse](s"$k8sApiServerURL/persistentvolumes")
            .map(_.items)
            .map { items =>
              items.map { item =>
                ContainerVolume(
                  name = item.metadata.name,
                  driver = item.spec.volumeMode,
                  path = item.spec.hostPath.path,
                  nodeName = item.spec.nodeAffinity
                    .map(_.required.nodeSelectorTerms.head.matchExpressions.head.values.head)
                    .getOrElse("Unknown")
                )
              }
            }
        }
      }
    }
  }

  private[this] def httpExecutor = HttpExecutor.SINGLETON

  private[agent] val K8S_KIND_NAME = "K8S"

  private[k8s] def cpuUsedCalc(usedValue: String, totalValue: Int): Double = {
    //totalValue vairable value unit is core
    if (usedValue.endsWith("n"))
      usedValue.replace("n", "").toLong / (1000000000.0 * totalValue) // 1 core = 1000*1000*1000 nanocores
    else if (usedValue.endsWith("u"))
      usedValue.replace("u", "").toLong / (1000000.0 * totalValue) // 1 core = 1000*1000 u
    else if (usedValue.endsWith("m"))
      usedValue.replace("m", "").toLong / (1000.0 * totalValue) // 1 core = 1000 millicores
    else
      throw new IllegalArgumentException(s"The cpu used value ${usedValue} doesn't converter long type")
  }

  private[k8s] def memoryUsedCalc(usedValue: String, totalValue: Long): Double = {
    //totalValue variable value unit is KB
    if (usedValue.endsWith("Ki"))
      usedValue.replace("Ki", "").toDouble / totalValue
    else if (usedValue.endsWith("Mi"))
      usedValue.replace("Mi", "").toDouble * 1024 / totalValue // 1 Mi = 2^10 Ki
    else if (usedValue.endsWith("Gi"))
      usedValue.replace("Gi", "").toDouble * 1024 * 1024 / totalValue // 1 Gi = 2^20 Ki
    else if (usedValue.endsWith("Ti"))
      usedValue.replace("Ti", "").toDouble * 1024 * 1024 * 1024 / totalValue // 1 Ti = 2^30 Ki
    else if (usedValue.endsWith("Pi"))
      usedValue.replace("Pi", "").toDouble * 1024 * 1024 * 1024 * 1024 / totalValue // 1 Pi = 2^40 Ki
    else if (usedValue.endsWith("Ei"))
      usedValue.replace("Ei", "").toDouble * 1024 * 1024 * 1024 * 1024 * 1024 / totalValue // 1 Ei = 2^50 Ei
    else
      throw new IllegalArgumentException(s"The memory used value ${usedValue} doesn't converter double type")
  }

  trait ContainerCreator extends ContainerClient.ContainerCreator {
    def pullImagePolicy(imagePullPolicy: ImagePullPolicy): ContainerCreator
    def restartPolicy(restartPolicy: RestartPolicy): ContainerCreator
  }
}
