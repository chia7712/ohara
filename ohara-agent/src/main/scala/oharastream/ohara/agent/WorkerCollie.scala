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

package oharastream.ohara.agent
import java.util.Objects

import com.typesafe.scalalogging.Logger
import oharastream.ohara.agent.docker.ContainerState
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ContainerApi.{ContainerInfo, PortMapping}
import oharastream.ohara.client.configurator.FileInfoApi.FileInfo
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.client.configurator.VolumeApi.Volume
import oharastream.ohara.client.configurator.WorkerApi
import oharastream.ohara.client.configurator.WorkerApi.{Creation, WorkerClusterInfo}
import oharastream.ohara.client.kafka.ConnectorAdmin

import scala.concurrent.{ExecutionContext, Future}

trait WorkerCollie extends Collie {
  private[this] val log          = Logger(classOf[WorkerCollie])
  override val kind: ClusterKind = ClusterKind.WORKER
  // TODO: remove this hard code (see #2957)
  // this path must be equal to the config path defined by docker/wk.sh
  private[this] val configPath: String = s"/home/ohara/default/config/worker.config"

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each worker container should assign "docker host name/port" to advertised name/port
    * 4) add broker routes to all worker containers (worker needs to connect to broker cluster)
    * 5) Add worker routes to all worker containers
    * 6) update existed containers (if we are adding new node into a running cluster)
    * @return description of worker cluster
    */
  override def creator: WorkerCollie.ClusterCreator = (executionContext, creation) => {
    implicit val exec: ExecutionContext = executionContext
    val resolveRequiredInfos = for {
      allNodes <- dataCollie.valuesByNames[Node](creation.nodeNames)
      existentNodes <- clusters().map(_.find(_.key == creation.key)).flatMap {
        case Some(value) =>
          dataCollie
            .valuesByNames[Node](value.nodeNames)
            .map(nodes => nodes.map(node => node -> value.containers.find(_.nodeName == node.hostname).get).toMap)
        case None => Future.successful(Map.empty[Node, ContainerInfo])
      }
      brokerClusterInfo <- dataCollie.value[BrokerClusterInfo](creation.brokerClusterKey)
      pluginInfos       <- dataCollie.values[FileInfo](creation.pluginKeys)
      sharedJarInfos    <- dataCollie.values[FileInfo](creation.sharedJarKeys)
      volumeMaps <- Future
        .traverse(creation.volumeMaps.keySet)(dataCollie.value[Volume])
        .map(_.map(v => v -> creation.volumeMaps(v.key)).toMap)
    } yield (
      existentNodes,
      allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
      brokerClusterInfo,
      pluginInfos,
      sharedJarInfos,
      volumeMaps
    )

    resolveRequiredInfos.flatMap {
      case (existentNodes, newNodes, brokerClusterInfo, pluginInfos, sharedJarInfos, volumeMaps) =>
        val routes = resolveHostNames(
          (existentNodes.keys.map(_.hostname)
            ++ newNodes.map(_.hostname)
            ++ brokerClusterInfo.nodeNames).toSet
          // make sure the worker can connect to configurator for downloading jars
          // Normally, the jar host name should be resolvable by worker since
          // we should add the "hostname" to configurator for most cases...
          // This is for those configurators that have no hostname (for example, temp configurator)
            ++ pluginInfos.map(_.url.get.getHost).toSet
        ) ++ creation.routes
        val successfulContainersFuture =
          if (newNodes.isEmpty) Future.successful(Seq.empty)
          else {
            val brokers =
              brokerClusterInfo.nodeNames.map(nodeName => s"$nodeName:${brokerClusterInfo.clientPort}").mkString(",")

            // ssh connection is slow so we submit request by multi-thread
            Future.sequence(newNodes.map { newNode =>
              val containerInfo = ContainerInfo(
                nodeName = newNode.name,
                id = Collie.UNKNOWN,
                imageName = creation.imageName,
                // this fake container will be cached before refreshing cache so we make it running.
                // other, it will be filtered later ...
                state = ContainerState.RUNNING.name,
                kind = Collie.UNKNOWN,
                name = Collie.containerName(creation.group, creation.name, kind),
                size = -1,
                portMappings = creation.ports
                  .map(
                    port =>
                      PortMapping(
                        hostIp = Collie.UNKNOWN,
                        hostPort = port,
                        containerPort = port
                      )
                  )
                  .toSeq,
                environments = Map(
                  "KAFKA_JMX_OPTS" -> (s"-Dcom.sun.management.jmxremote" +
                    s" -Dcom.sun.management.jmxremote.authenticate=false" +
                    s" -Dcom.sun.management.jmxremote.ssl=false" +
                    s" -Dcom.sun.management.jmxremote.port=${creation.jmxPort}" +
                    s" -Dcom.sun.management.jmxremote.rmi.port=${creation.jmxPort}" +
                    s" -Djava.rmi.server.hostname=${newNode.hostname}"),
                  "KAFKA_HEAP_OPTS" -> s"-Xms${creation.initHeap}M -Xmx${creation.maxHeap}M",
                  // define the urls as string list so as to simplify the script for worker
                  "WORKER_PLUGIN_URLS"     -> pluginInfos.map(_.url.get.toURI.toASCIIString).mkString(","),
                  "WORKER_SHARED_JAR_URLS" -> sharedJarInfos.map(_.url.get.toURI.toASCIIString).mkString(",")
                ) ++ creation.jvmPerformanceOptions
                  .map(options => Map("KAFKA_JVM_PERFORMANCE_OPTS" -> options))
                  .getOrElse(Map.empty),
                hostname = Collie.containerHostName(creation.group, creation.name, kind)
              )

              /**
                * Construct the required configs for current container
                * we will loop all the files in FILE_DATA of arguments : --file A --file B --file C
                * the format of A, B, C should be file_name=k1=v1,k2=v2,k3,k4=v4...
                */
              val arguments = ArgumentsBuilder()
                .mainConfigFile(configPath)
                .file(configPath)
                .append("bootstrap.servers", brokers)
                .append(WorkerApi.GROUP_ID_KEY, creation.groupId)
                .append(WorkerApi.CONFIG_TOPIC_NAME_KEY, creation.configTopicName)
                .append(WorkerApi.CONFIG_TOPIC_REPLICATIONS_KEY, creation.configTopicReplications)
                .append(WorkerApi.OFFSET_TOPIC_NAME_KEY, creation.offsetTopicName)
                .append(WorkerApi.OFFSET_TOPIC_PARTITIONS_KEY, creation.offsetTopicPartitions)
                .append(WorkerApi.OFFSET_TOPIC_REPLICATIONS_KEY, creation.offsetTopicReplications)
                .append(WorkerApi.STATUS_TOPIC_NAME_KEY, creation.statusTopicName)
                .append(WorkerApi.STATUS_TOPIC_PARTITIONS_KEY, creation.statusTopicPartitions)
                .append(WorkerApi.STATUS_TOPIC_REPLICATIONS_KEY, creation.statusTopicReplications)
                .append(WorkerApi.COMPRESSION_TYPE_KEY, creation.compressionType.toString)
                .append("rest.port", creation.clientPort)
                .append("rest.advertised.host.name", newNode.hostname)
                .append("rest.advertised.port", creation.clientPort)
                // We offers the kafka recommend settings since we always overwrite the converter in starting connector
                // (see ConnectorFormatter)
                // If users want to deploy connectors manually, this default settings can simplify their life from coming
                // across the schema error :)
                .append("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .append("key.converter.schemas.enable", true)
                .append("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .append("value.converter.schemas.enable", true)
                /**
                  * this config enables us to custom producer/consumer for source/sink.
                  */
                .append("connector.client.config.override.policy", "All")
                .done
                .build
              doCreator(
                executionContext = executionContext,
                containerInfo = containerInfo,
                node = newNode,
                routes = routes,
                arguments = arguments,
                volumeMaps = volumeMaps
              ).map(_ => Some(containerInfo))
                .recover {
                  case e: Throwable =>
                    log.error(s"failed to create worker container on ${newNode.hostname}", e)
                    None
                }
            })
          }
        successfulContainersFuture.map(_.flatten.toSeq).flatMap { successfulContainers =>
          val aliveContainers = existentNodes.values.toSeq ++ successfulContainers
          postCreate(
            clusterStatus = ClusterStatus(
              group = creation.group,
              name = creation.name,
              containers = aliveContainers,
              kind = ClusterKind.WORKER,
              state = toClusterState(aliveContainers),
              error = None
            ),
            existentNodes = existentNodes,
            routes = routes,
            volumeMaps = volumeMaps
          )
        }
    }
  }

  /**
    * Create a worker client according to passed cluster.
    * Noted: this method is placed at collie so as to enable fake collie be available to route.
    * @param workerClusterInfo target cluster
    * @return worker client
    */
  def connectorAdmin(
    workerClusterInfo: WorkerClusterInfo
  )(implicit executionContext: ExecutionContext): Future[ConnectorAdmin] =
    cluster(workerClusterInfo.key).map(_ => ConnectorAdmin(workerClusterInfo))
}

object WorkerCollie {
  trait ClusterCreator extends Collie.ClusterCreator with WorkerApi.Request {
    override def create(): Future[Unit] =
      doCreate(
        executionContext = Objects.requireNonNull(executionContext),
        creation = creation
      )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
