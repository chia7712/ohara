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
import oharastream.ohara.client.configurator.StreamApi
import oharastream.ohara.client.configurator.StreamApi.Creation
import oharastream.ohara.client.configurator.VolumeApi.Volume
import oharastream.ohara.stream.Stream
import oharastream.ohara.stream.config.StreamSetting
import spray.json.JsString

import scala.concurrent.{ExecutionContext, Future}

/**
  * An interface of controlling stream cluster.
  * It isolates the implementation of container manager from Configurator.
  */
trait StreamCollie extends Collie {
  private[this] val log = Logger(classOf[StreamCollie])
  override def creator: StreamCollie.ClusterCreator =
    (executionContext, creation) => {
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
        fileInfo          <- dataCollie.value[FileInfo](creation.jarKey)
        volumeMaps <- Future
          .traverse(creation.volumeMaps.keySet)(dataCollie.value[Volume])
          .map(_.map(v => v -> creation.volumeMaps(v.key)).toMap)
      } yield (
        existentNodes,
        allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
        brokerClusterInfo,
        fileInfo,
        volumeMaps
      )

      resolveRequiredInfos
        .map {
          case (existentNodes, newNodes, brokerClusterInfo, fileInfo, volumeMaps) =>
            if (existentNodes.nonEmpty)
              throw new UnsupportedOperationException(s"stream collie doesn't support to add node to a running cluster")
            else (newNodes, brokerClusterInfo, fileInfo, volumeMaps)
        }
        .flatMap {
          case (newNodes, brokerClusterInfo, fileInfo, volumeMaps) =>
            val routes = resolveHostNames(
              (newNodes.map(_.hostname)
                ++ brokerClusterInfo.nodeNames
              // make sure the stream can connect to configurator
                ++ Seq(fileInfo.url.get.getHost)).toSet
            ) ++ creation.routes
            val successfulContainersFuture =
              if (newNodes.isEmpty) Future.successful(Seq.empty)
              else {
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
                      "JMX_PORT"     -> creation.jmxPort.toString,
                      "JMX_HOSTNAME" -> newNode.hostname,
                      // define the urls as string list so as to simplify the script for stream
                      "STREAM_JAR_URLS" -> fileInfo.url.get.toURI.toASCIIString,
                      "OHARA_HEAP_OPTS" -> s"-Xms${creation.initHeap}M -Xmx${creation.maxHeap}M"
                    ),
                    // we should set the hostname to container name in order to avoid duplicate name with other containers
                    hostname = Collie.containerHostName(creation.group, creation.name, kind)
                  )

                  val arguments =
                    Seq(classOf[Stream].getName) ++ creation.raw
                      .map {
                        case (k, v) =>
                          k -> (v match {
                            // the string in json representation has quote in the beginning and end.
                            // we don't like the quotes since it obstruct us to cast value to pure string.
                            case JsString(s) => StreamSetting.toEnvString(s)
                            // save the json string for all settings
                            // StreamDefUtils offers the helper method to turn them back.
                            case _ => StreamSetting.toEnvString(v.toString)
                          })
                      }
                      .map {
                        case (k, v) => s"$k=$v"
                      }

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
                        log.error(s"failed to create stream container on ${newNode.hostname}", e)
                        None
                    }
                })
              }

            successfulContainersFuture.map(_.flatten.toSeq).flatMap { aliveContainers =>
              postCreate(
                clusterStatus = ClusterStatus(
                  group = creation.group,
                  name = creation.name,
                  containers = aliveContainers,
                  kind = ClusterKind.STREAM,
                  state = toClusterState(aliveContainers),
                  error = None
                ),
                existentNodes = Map.empty,
                routes = routes,
                volumeMaps = volumeMaps
              )
            }
        }
    }

  override val kind: ClusterKind = ClusterKind.STREAM
}

object StreamCollie {
  trait ClusterCreator extends Collie.ClusterCreator with StreamApi.Request {
    override def create(): Future[Unit] = doCreate(
      executionContext = Objects.requireNonNull(executionContext),
      creation = creation
    )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
