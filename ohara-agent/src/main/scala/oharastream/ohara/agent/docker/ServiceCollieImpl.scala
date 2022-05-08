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

import java.util.concurrent.ExecutorService

import oharastream.ohara.agent.{ClusterKind, ClusterStatus, _}
import oharastream.ohara.client.configurator.ContainerApi.ContainerInfo
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.Releasable

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

// accessible to configurator
private[ohara] class ServiceCollieImpl(cacheTimeout: Duration, dataCollie: DataCollie, cacheThreadPool: ExecutorService)
    extends ServiceCollie {
  override val containerClient: DockerClient = DockerClient(dataCollie)

  private[this] val clusterCache: ServiceCache = ServiceCache.builder
    .frequency(cacheTimeout)
    // TODO: 5 * timeout is enough ??? by chia
    .supplier(() => Await.result(doClusters(ExecutionContext.fromExecutor(cacheThreadPool)), cacheTimeout * 5))
    // Giving some time to process to complete the build and then we can remove it from cache safety.
    .lazyRemove(cacheTimeout)
    .build()

  override val zookeeperCollie: ZookeeperCollie = new BasicCollieImpl(dataCollie, containerClient, clusterCache)
    with ZookeeperCollie
  override val brokerCollie: BrokerCollie = new BasicCollieImpl(dataCollie, containerClient, clusterCache)
    with BrokerCollie
  override val workerCollie: WorkerCollie = new BasicCollieImpl(dataCollie, containerClient, clusterCache)
    with WorkerCollie
  override val streamCollie: StreamCollie = new BasicCollieImpl(dataCollie, containerClient, clusterCache)
    with StreamCollie
  override val shabondiCollie: ShabondiCollie = new BasicCollieImpl(dataCollie, containerClient, clusterCache)
    with ShabondiCollie

  private[this] def doClusters(
    implicit executionContext: ExecutionContext
  ): Future[Seq[ClusterStatus]] =
    containerClient
      .containers()
      .map { allContainers =>
        def parse(
          kind: ClusterKind,
          toClusterStatus: (ObjectKey, Seq[ContainerInfo]) => ClusterStatus
        ): Seq[ClusterStatus] =
          allContainers
            .filter(container => Collie.matched(container.name, kind))
            .map(container => Collie.objectKeyOfContainerName(container.name) -> container)
            .groupBy(_._1)
            .map {
              case (clusterKey, value) => clusterKey -> value.map(_._2)
            }
            .map {
              case (clusterKey, containers) => toClusterStatus(clusterKey, containers)
            }
            .toSeq

        parse(ClusterKind.ZOOKEEPER, zookeeperCollie.toStatus) ++
          parse(ClusterKind.BROKER, brokerCollie.toStatus) ++
          parse(ClusterKind.WORKER, workerCollie.toStatus) ++
          parse(ClusterKind.STREAM, streamCollie.toStatus) ++
          parse(ClusterKind.SHABONDI, shabondiCollie.toStatus)
      }

  override def close(): Unit = {
    Releasable.close(containerClient)
    Releasable.close(clusterCache)
    Releasable.close(() => cacheThreadPool.shutdownNow())
  }

  /**
    * The default implementation has the following checks.
    * 1) run hello-world image
    * 2) check existence of hello-world
    */
  override def verifyNode(node: Node)(implicit executionContext: ExecutionContext): Future[String] =
    containerClient
      .resources()
      .map { resources =>
        if (resources.getOrElse(node.hostname, Seq.empty).nonEmpty)
          s"succeed to check the docker resources on ${node.name}"
        else throw new IllegalStateException(s"the docker on ${node.hostname} is unavailable")
      }
}
