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

import java.util.concurrent.ConcurrentHashMap

import oharastream.ohara.agent.container.{ContainerClient, ContainerName, ContainerVolume}
import oharastream.ohara.agent.{DataCollie, ServiceCollie}
import oharastream.ohara.client.configurator.NodeApi.{Node, Resource}
import oharastream.ohara.client.configurator.{BrokerApi, ContainerApi, ShabondiApi, StreamApi, WorkerApi, ZookeeperApi}
import oharastream.ohara.common.util.CommonUtils

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * It doesn't involve any running cluster but save all description in memory
  */
private[configurator] class FakeServiceCollie(
  dataCollie: DataCollie,
  bkConnectionProps: String,
  wkConnectionProps: String
) extends ServiceCollie {
  def this(dataCollie: DataCollie) = this(dataCollie, null, null)

  private[this] val client: ContainerClient = new ContainerClient {
    override def containers()(implicit executionContext: ExecutionContext): Future[Seq[ContainerApi.ContainerInfo]] =
      throw new UnsupportedOperationException("this is fake container client")

    override def remove(name: String)(implicit executionContext: ExecutionContext): Future[Unit] =
      throw new UnsupportedOperationException("this is fake container client")

    override def forceRemove(name: String)(implicit executionContext: ExecutionContext): Future[Unit] =
      throw new UnsupportedOperationException("this is fake container client")

    override def log(name: String, sinceSeconds: Option[Long])(
      implicit executionContext: ExecutionContext
    ): Future[Map[ContainerName, String]] =
      throw new UnsupportedOperationException("this is fake container client")

    override def containerCreator: ContainerClient.ContainerCreator =
      throw new UnsupportedOperationException("this is fake container client")

    override def imageNames()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[String]]] =
      throw new UnsupportedOperationException("this is fake container client")

    override def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]] =
      throw new UnsupportedOperationException("this is fake container client")

    /**
      * volume name -> node name -> volume
      */
    private[this] val existentVolumes = new ConcurrentHashMap[String, ConcurrentHashMap[String, ContainerVolume]]()

    override def volumeCreator: ContainerClient.VolumeCreator =
      (nodeName: String, name: String, path: String, _: ExecutionContext) => {
        val volume = ContainerVolume(
          name = name,
          driver = "fake",
          path = path,
          nodeName = nodeName
        )

        val updated = existentVolumes
          .computeIfAbsent(name, _ => new ConcurrentHashMap[String, ContainerVolume])
          .computeIfAbsent(volume.nodeName, _ => volume)
        if (updated == volume) Future.unit
        else Future.failed(new IllegalArgumentException(s"$name exists on ${volume.nodeName}!!!"))
      }

    override def volumes()(implicit executionContext: ExecutionContext): Future[Seq[ContainerVolume]] =
      Future.successful(existentVolumes.values().asScala.flatMap(_.asScala.values).toSeq)

    override def removeVolumes(name: String)(implicit executionContext: ExecutionContext): Future[Unit] = {
      existentVolumes.remove(name)
      Future.unit
    }
    override def close(): Unit = existentVolumes.clear()
  }

  override val zookeeperCollie: FakeZookeeperCollie = new FakeZookeeperCollie(client, dataCollie)

  override val brokerCollie: FakeBrokerCollie = new FakeBrokerCollie(client, dataCollie, bkConnectionProps)

  override val workerCollie: FakeWorkerCollie = new FakeWorkerCollie(client, dataCollie, wkConnectionProps)

  override val streamCollie: FakeStreamCollie = new FakeStreamCollie(client, dataCollie)

  override val shabondiCollie: FakeShabondiCollie = new FakeShabondiCollie(client, dataCollie)

  override def close(): Unit = {
    // do nothing
  }

  override def imageNames()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[String]]] =
    dataCollie.values[Node]().map { nodes =>
      nodes
        .map(
          _.hostname -> Seq(
            ZookeeperApi.IMAGE_NAME_DEFAULT,
            BrokerApi.IMAGE_NAME_DEFAULT,
            WorkerApi.IMAGE_NAME_DEFAULT,
            StreamApi.IMAGE_NAME_DEFAULT,
            ShabondiApi.IMAGE_NAME_DEFAULT
          )
        )
        .toMap
    }

  override def verifyNode(node: Node)(implicit executionContext: ExecutionContext): Future[String] =
    Future.successful(s"This is fake mode so we didn't test connection actually...")

  override def containerNames()(implicit executionContext: ExecutionContext): Future[Seq[ContainerName]] =
    Future.successful(Seq.empty)

  override def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]] =
    dataCollie
      .values[Node]()
      .map(
        nodes =>
          nodes
            .map(node => {
              val cpuResource    = Resource.cpu(32, Option(positiveValue(CommonUtils.randomDouble())))
              val memoryResource = Resource.memory(137438953472L, Option(positiveValue(CommonUtils.randomDouble())))
              (node.hostname, Seq(cpuResource, memoryResource))
            })
            .toMap
      )

  private[this] def positiveValue(value: Double): Double = Math.abs(value)

  override def log(containerName: String, sinceSeconds: Option[Long])(
    implicit executionContext: ExecutionContext
  ): Future[Map[ContainerName, String]] =
    Future.failed(new NoSuchElementException)

  override def containerClient: ContainerClient = client

  /**
    * @return true if this service is built with embedded kafka. Otherwise, false
    */
  def embedded: Boolean = wkConnectionProps != null && bkConnectionProps != null
}
