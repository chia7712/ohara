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
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.client.configurator.ShabondiApi
import oharastream.ohara.client.configurator.ShabondiApi.Creation
import oharastream.ohara.shabondi.common.ShabondiUtils
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

trait ShabondiCollie extends Collie {
  protected val log = Logger(classOf[ShabondiCollie])

  override val kind: ClusterKind = ClusterKind.SHABONDI

  override def creator: ShabondiCollie.ClusterCreator =
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
      } yield (
        existentNodes,
        allNodes.filterNot(node => existentNodes.exists(_._1.hostname == node.hostname)),
        brokerClusterInfo
      )

      // TODO: Shabondi should support dynamically scaling nodes
      resolveRequiredInfos.foreach {
        case (existentNodes, _, _) =>
          if (existentNodes.nonEmpty) {
            throw new UnsupportedOperationException(
              s"Shabondi collie doesn't support to add node to a running cluster"
            )
          }
      }

      resolveRequiredInfos.flatMap {
        case (existentNodes, newNodes, brokerClusterInfo) =>
          val routes = resolveHostNames(
            (existentNodes.keys.map(_.hostname)
              ++ newNodes.map(_.hostname)
              ++ brokerClusterInfo.nodeNames).toSet
          ) ++ creation.routes

          val successfulContainersFuture =
            if (newNodes.isEmpty) Future.successful(Seq.empty)
            else {
              Future.sequence(newNodes.map { newNode =>
                val env = Map(
                  "JMX_PORT"        -> creation.jmxPort.toString,
                  "JMX_HOSTNAME"    -> newNode.hostname,
                  "OHARA_HEAP_OPTS" -> s"-Xms${creation.initHeap}M -Xmx${creation.maxHeap}M"
                )

                val containerInfo = newContainerInfo(newNode, creation, env)
                val arguments = creation.raw.map {
                  case (k, v) =>
                    val value = v match {
                      case JsString(s) => ShabondiUtils.escape(s)
                      case _           => ShabondiUtils.escape(v.toString)
                    }
                    k + "=" + value
                }.toSeq
                doCreator(
                  executionContext = executionContext,
                  containerInfo = containerInfo,
                  node = newNode,
                  routes = routes,
                  arguments = Seq(creation.shabondiClass) ++ arguments,
                  // shabondi does not use volumes
                  volumeMaps = Map.empty
                ).map(_ => Some(containerInfo))
                  .recover {
                    case e: Throwable =>
                      log.error(s"failed to create stream container on ${newNode.hostname}", e)
                      None
                  }
              })
            }
          successfulContainersFuture
            .map(_.flatten.toSeq)
            .flatMap { aliveContainers =>
              val clusterStatus =
                ClusterStatus(
                  group = creation.group,
                  name = creation.name,
                  kind = ClusterKind.SHABONDI,
                  state = toClusterState(aliveContainers),
                  error = None,
                  containers = aliveContainers
                )
              postCreate(
                clusterStatus = clusterStatus,
                existentNodes = existentNodes,
                routes = routes,
                // shabondi does not use volumes
                volumeMaps = Map.empty
              )
            }
      }
    }

  private def newContainerInfo(node: Node, creation: Creation, env: Map[String, String]): ContainerInfo =
    ContainerInfo(
      nodeName = node.name,
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
      environments = env,
      hostname = Collie.containerHostName(creation.group, creation.name, kind)
    )
}

object ShabondiCollie {
  trait ClusterCreator extends Collie.ClusterCreator with ShabondiApi.Request {
    override def create(): Future[Unit] = doCreate(
      executionContext = Objects.requireNonNull(executionContext),
      creation = creation
    )

    protected def doCreate(executionContext: ExecutionContext, creation: Creation): Future[Unit]
  }
}
