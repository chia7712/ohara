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

import java.util.concurrent.{ConcurrentSkipListMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import oharastream.ohara.agent._
import oharastream.ohara.agent.container.ContainerName
import oharastream.ohara.agent.docker.ContainerState
import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ConnectorApi.ConnectorInfo
import oharastream.ohara.client.configurator.ContainerApi.{ContainerInfo, PortMapping}
import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.client.configurator.ShabondiApi.ShabondiClusterInfo
import oharastream.ohara.client.configurator.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.TopicApi.{TopicInfo, TopicState}
import oharastream.ohara.client.configurator.VolumeApi.Volume
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.{ClusterInfo, ClusterState, ConnectorApi, NodeApi}
import oharastream.ohara.common.annotations.VisibleForTesting
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.metrics.BeanChannel
import oharastream.ohara.metrics.basic.{Counter, CounterMBean}
import oharastream.ohara.metrics.kafka.TopicMeter

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
private[configurator] abstract class FakeCollie(val dataCollie: DataCollie) extends Collie {
  @VisibleForTesting
  protected[configurator] val clusterCache = new ConcurrentSkipListMap[ObjectKey, ClusterStatus]()

  /**
    * update the in-memory cluster status and container infos
    * @return cluster status
    */
  private[configurator] def addCluster(
    key: ObjectKey,
    kind: ClusterKind,
    nodeNames: Set[String],
    imageName: String,
    ports: Set[Int]
  ): ClusterStatus =
    clusterCache.put(
      key,
      ClusterStatus(
        group = key.group(),
        name = key.name(),
        state = Some(ClusterState.RUNNING),
        error = None,
        kind = kind,
        containers = nodeNames
          .map(
            nodeName =>
              ContainerInfo(
                nodeName = nodeName,
                id = CommonUtils.randomString(10),
                imageName = imageName,
                state = ContainerState.RUNNING.name,
                kind = "FAKE",
                name = CommonUtils.randomString(10),
                size = -1,
                portMappings = ports.map(p => PortMapping("fake", p, p)).toSeq,
                environments = Map.empty,
                hostname = CommonUtils.randomString(10)
              )
          )
          .toSeq
      )
    )

  /**
    * Test this collie is running on embedded mode or not by checking local JVM metrics.
    * @return true if run on embedded mode
    */
  private[configurator] def isEmbedded: Boolean =
    !BeanChannel.local().topicMeters().isEmpty || !BeanChannel.local().counterMBeans().isEmpty

  // fake topicMeter metrics
  private def fakeTopicMeter(key: ObjectKey) =
    TopicMeter
      .builder()
      .topicName(key.toPlain)
      .catalog(TopicMeter.Catalog.MessagesInPerSec)
      .rateUnit(TimeUnit.SECONDS)
      .build()

  // fake counter metrics
  private def fakeCounter(key: ObjectKey) =
    Counter
      .builder()
      .key(key)
      .item("fake counter")
      .value(CommonUtils.randomInteger().toLong)
      .build()

  override def exist(objectKey: ObjectKey)(implicit executionContext: ExecutionContext): Future[Boolean] =
    Future.successful(clusterCache.keySet.asScala.contains(objectKey))

  override protected def doRemove(clusterInfo: ClusterStatus, beRemovedContainer: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    val previous = clusterCache.get(clusterInfo.key)
    if (previous == null) Future.unit
    else {
      val newContainers =
        previous.containers.filterNot(container => beRemovedContainer.exists(_.name == container.name))
      if (newContainers.isEmpty) clusterCache.remove(clusterInfo.key)
      else clusterCache.put(previous.key, previous.copy(containers = newContainers))
      // return true if it does remove something
      Future.successful(newContainers.size != previous.containers.size)
    }
  }

  override def log(objectKey: ObjectKey, sinceSeconds: Option[Long])(
    implicit executionContext: ExecutionContext
  ): Future[Map[ContainerName, String]] =
    exist(objectKey).flatMap(if (_) Future.successful {
      clusterCache.asScala
        .find(_._1 == objectKey)
        .get
        ._2
        .containers
        .map(
          container =>
            new ContainerName(
              id = container.id,
              name = container.name,
              nodeName = container.nodeName,
              imageName = container.imageName
            )
        )
        .map(_ -> "fake log")
        .toMap
    } else Future.failed(new NoSuchClusterException(s"$objectKey doesn't exist")))

  override def clusters()(
    implicit executionContext: ExecutionContext
  ): Future[Seq[ClusterStatus]] =
    Future.successful(clusterCache.asScala.values.toSeq)

  private[this] val _forceRemoveCount = new AtomicInteger(0)
  override protected def doForceRemove(clusterInfo: ClusterStatus, containerInfos: Seq[ContainerInfo])(
    implicit executionContext: ExecutionContext
  ): Future[Unit] =
    try doRemove(clusterInfo, containerInfos)
    finally _forceRemoveCount.incrementAndGet()

  // In fake mode, the cluster state should be running since we add "running containers" always
  override protected def toClusterState(containers: Seq[ContainerInfo]): Option[ClusterState] =
    Some(ClusterState.RUNNING)

  def forceRemoveCount: Int = _forceRemoveCount.get()

  override protected def topicMeters(cluster: ClusterInfo): Map[String, Seq[TopicMeter]] = cluster match {
    case _: BrokerClusterInfo =>
      if (isEmbedded) {
        // using local jvm on embedded mode
        Map(CommonUtils.hostname() -> BeanChannel.local().topicMeters().asScala.toSeq)
      } else {
        if (clusterCache.containsKey(cluster.key)) {
          import scala.concurrent.ExecutionContext.Implicits.global
          val topicKeys =
            Await.result(
              dataCollie
                .values[TopicInfo]()
                .map(infos => infos.filter(info => info.state.contains(TopicState.RUNNING)).map(info => info.key)),
              Duration.Inf
            )
          topicKeys.map(topicKey => CommonUtils.hostname() -> Seq(fakeTopicMeter(topicKey))).toMap
        } else Map.empty
      }
    case _ => Map.empty
  }

  override protected def counterMBeans(cluster: ClusterInfo): Map[String, Seq[CounterMBean]] = cluster match {
    case _: BrokerClusterInfo =>
      /**
        * the metrics we fetch from kafka are only topic metrics so we skip the other beans
        */
      Map.empty
    case _ @(_: StreamClusterInfo | _: ShabondiClusterInfo) =>
      // we fake counters since stream is not really running in fake collie mode
      if (clusterCache.containsKey(cluster.key)) {
        Map(CommonUtils.hostname() -> Seq(fakeCounter(cluster.key)))
      } else Map.empty
    case _ @(_: WorkerClusterInfo) =>
      if (isEmbedded) {
        // using local jvm on embedded mode
        Map(CommonUtils.hostname() -> BeanChannel.local().counterMBeans().asScala.toSeq)
      } else {
        if (clusterCache.containsKey(cluster.key)) {
          import scala.concurrent.ExecutionContext.Implicits.global
          val connectorKeys =
            Await.result(
              dataCollie
                .values[ConnectorInfo]()
                .map(
                  infos => infos.filter(info => info.state.contains(ConnectorApi.State.RUNNING)).map(info => info.key)
                ),
              Duration.Inf
            )
          connectorKeys.map(connectorKey => CommonUtils.hostname() -> Seq(fakeCounter(connectorKey))).toMap
        } else Map.empty
      }
    case _ =>
      // we don't care for the fake mode since both fake mode and embedded mode are run on local jvm
      Map(CommonUtils.hostname() -> BeanChannel.local().counterMBeans().asScala.toSeq)
  }

  override protected def doCreator(
    executionContext: ExecutionContext,
    containerInfo: ContainerInfo,
    node: NodeApi.Node,
    route: Map[String, String],
    arguments: Seq[String],
    volumeMaps: Map[Volume, String]
  ): Future[Unit] =
    throw new UnsupportedOperationException("fake collie doesn't support to doCreator function")

  override def postCreate(
    clusterStatus: ClusterStatus,
    existentNodes: Map[Node, ContainerInfo],
    routes: Map[String, String],
    volumeMaps: Map[Volume, String]
  )(implicit executionContext: ExecutionContext): Future[Unit] = Future.unit
}
