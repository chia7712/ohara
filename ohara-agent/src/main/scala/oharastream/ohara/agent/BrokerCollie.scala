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
import oharastream.ohara.client.configurator.BrokerApi
import oharastream.ohara.client.configurator.BrokerApi.{BrokerClusterInfo, Creation}
import oharastream.ohara.client.configurator.ContainerApi.{ContainerInfo, PortMapping}
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.client.configurator.VolumeApi.Volume
import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.kafka.TopicAdmin

import scala.concurrent.{ExecutionContext, Future}

trait BrokerCollie extends Collie {
  private[this] val log = Logger(classOf[BrokerCollie])

  override val kind: ClusterKind = ClusterKind.BROKER
  // TODO: remove this hard code (see #2957)
  // this path must be equal to the config path defined by docker/bk.sh
  private[this] val configPath: String = s"/home/ohara/default/config/broker.config"

  /**
    * This is a complicated process. We must address following issues.
    * 1) check the existence of cluster
    * 2) check the existence of nodes
    * 3) Each broker container should assign "docker host name/port" to advertised name/port
    * 4) add zookeeper routes to all broker containers (broker needs to connect to zookeeper cluster)
    * 5) Add broker routes to all broker containers
    * 6) update existed containers (if we are adding new node into a running cluster)
    * @return creator of broker cluster
    */
  override def creator: BrokerCollie.ClusterCreator = (executionContext, creation) => {
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
      zookeeperClusterInfo <- dataCollie.value[ZookeeperClusterInfo](creation.zookeeperClusterKey)
      volumeMaps <- Future
        .traverse(creation.volumeMaps.keySet)(dataCollie.value[Volume])
        .map(_.map(v => v -> creation.volumeMaps(v.key)).toMap)
    } yield (
      existentNodes,
      allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
      zookeeperClusterInfo,
      volumeMaps
    )

    resolveRequiredInfos.flatMap {
      case (existentNodes, newNodes, zookeeperClusterInfo, volumeMaps) =>
        val routes = resolveHostNames(
          (existentNodes.keys.map(_.hostname) ++ newNodes.map(_.hostname) ++ zookeeperClusterInfo.nodeNames).toSet
        ) ++ creation.routes
        val successfulContainersFuture =
          if (newNodes.isEmpty) Future.successful(Seq.empty)
          else {
            val zookeepers = zookeeperClusterInfo.nodeNames
              .map(nodeName => s"$nodeName:${zookeeperClusterInfo.clientPort}")
              .mkString(",")

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
                  "KAFKA_HEAP_OPTS" -> s"-Xms${creation.initHeap}M -Xmx${creation.maxHeap}M"
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
                .append("zookeeper.connect", zookeepers)
                .append(BrokerApi.LOG_DIRS_KEY, creation.dataFolders)
                .append(BrokerApi.NUMBER_OF_PARTITIONS_KEY, creation.numberOfPartitions)
                .append(
                  BrokerApi.NUMBER_OF_REPLICATIONS_4_OFFSETS_TOPIC_KEY,
                  creation.numberOfReplications4OffsetsTopic
                )
                .append(
                  BrokerApi.NUMBER_OF_REPLICATIONS_4_TRANSACTION_TOPIC_KEY,
                  creation.numberOfReplications4OffsetsTopic
                )
                .append(
                  BrokerApi.MIN_IN_SYNC_4_TOPIC_KEY,
                  creation.minInSync4Topic
                )
                .append(
                  BrokerApi.MIN_IN_SYNC_4_TRANSACTION_TOPIC_KEY,
                  creation.minInSync4TransactionTopic
                )
                .append(BrokerApi.NUMBER_OF_NETWORK_THREADS_KEY, creation.numberOfNetworkThreads)
                .append(BrokerApi.NUMBER_OF_IO_THREADS_KEY, creation.numberOfIoThreads)
                .append(BrokerApi.MAX_OF_POOL_MEMORY_BYTES, creation.maxOfPoolMemory)
                .append(BrokerApi.MAX_OF_REQUEST_MEMORY_BYTES, creation.maxOfRequestMemory)
                .append(s"listeners=PLAINTEXT://:${creation.clientPort}")
                .append(s"advertised.listeners=PLAINTEXT://${newNode.hostname}:${creation.clientPort}")
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
                    log.error(s"failed to create broker container on ${newNode.hostname}", e)
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
              kind = ClusterKind.BROKER,
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
    * Create a topic admin according to passed cluster.
    * Noted: the input cluster MUST be running. otherwise, a exception is returned.
    * @param brokerClusterInfo target cluster
    * @return topic admin
    */
  def topicAdmin(
    brokerClusterInfo: BrokerClusterInfo
  )(implicit executionContext: ExecutionContext): Future[TopicAdmin] =
    cluster(brokerClusterInfo.key).map(_ => TopicAdmin.of(brokerClusterInfo.connectionProps))
}

object BrokerCollie {
  trait ClusterCreator extends Collie.ClusterCreator with BrokerApi.Request {
    override def create(): Future[Unit] =
      doCreate(
        executionContext = Objects.requireNonNull(executionContext),
        creation = creation
      )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
