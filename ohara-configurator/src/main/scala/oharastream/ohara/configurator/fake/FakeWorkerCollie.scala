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

package oharastream.ohara.configurator.fake

import java.util.concurrent.ConcurrentSkipListMap

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.{ClusterKind, DataCollie, NoSuchClusterException, WorkerCollie}
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}

private[configurator] class FakeWorkerCollie(
  val containerClient: ContainerClient,
  dataCollie: DataCollie,
  wkConnectionProps: String
) extends FakeCollie(dataCollie)
    with WorkerCollie {
  /**
    * cache all connectors info in-memory so we should keep instance for each fake cluster.
    */
  private[this] val fakeClientCache = new ConcurrentSkipListMap[WorkerClusterInfo, FakeConnectorAdmin](
    (o1: WorkerClusterInfo, o2: WorkerClusterInfo) => o1.key.compareTo(o2.key)
  )
  override def creator: WorkerCollie.ClusterCreator =
    (_, creation) =>
      Future.successful(
        addCluster(
          key = creation.key,
          kind = ClusterKind.WORKER,
          nodeNames = creation.nodeNames ++ clusterCache.asScala
            .find(_._1 == creation.key)
            .map(_._2.nodeNames)
            .getOrElse(Set.empty),
          imageName = creation.imageName,
          ports = creation.ports
        )
      )

  override def connectorAdmin(
    cluster: WorkerClusterInfo
  )(implicit executionContext: ExecutionContext): Future[ConnectorAdmin] =
    if (wkConnectionProps != null)
      Future.successful(
        ConnectorAdmin.builder.workerClusterKey(ObjectKey.of("fake", "fake")).connectionProps(wkConnectionProps).build
      )
    else if (clusterCache.keySet().asScala.contains(cluster.key)) {
      val fake = FakeConnectorAdmin()
      val r    = fakeClientCache.putIfAbsent(cluster, fake)
      Future.successful(if (r == null) fake else r)
    } else
      Future.failed(new NoSuchClusterException(s"cluster:${cluster.key} is not running"))

  override def isEmbedded: Boolean = !CommonUtils.isEmpty(wkConnectionProps)
}
