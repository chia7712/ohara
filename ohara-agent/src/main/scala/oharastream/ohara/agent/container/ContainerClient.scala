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

package oharastream.ohara.agent.container

import java.util.Objects

import oharastream.ohara.agent.container.ContainerClient.{ContainerCreator, VolumeCreator}
import oharastream.ohara.client.configurator.ContainerApi.ContainerInfo
import oharastream.ohara.client.configurator.NodeApi.Resource
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.util.{CommonUtils, Releasable}

import scala.concurrent.{ExecutionContext, Future}

/**
  * a interface of Ohara containers. It is able to operate all containers created by ohara across all nodes.
  * For k8s mode, the entrypoint is k8s coordinator. By contrast, for docker mode, the implementation traverse all nodes
  * to handle requests.
  *
  * Noted: You should use this interface rather than docker client or k8s client since this interface consists of common
  * functions supported by all implementations, and your services SHOULD NOT assume the container implementations.
  */
trait ContainerClient extends Releasable {
  /**
    * @param executionContext thread pool
    * @return list all ohara containers
    */
  def containers()(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]]

  /**
    * get the container for specify name. Noted: the name should be unique in container env. However, the implementation
    * has no such limit possibly. However, you should NOT assume the implementation and then use the specification based
    * on specify implementation.
    *
    * @param name container name
    * @param executionContext thread pool
    * @return container
    */
  def containers(name: String)(implicit executionContext: ExecutionContext): Future[Seq[ContainerInfo]] =
    containers().map(_.filter(_.name == name))

  /**
    * @param executionContext thread pool
    * @return list all container names
    */
  def containerNames()(implicit executionContext: ExecutionContext): Future[Seq[ContainerName]] =
    containers().map(_.map { container =>
      new ContainerName(
        id = container.id,
        name = container.name,
        imageName = container.imageName,
        nodeName = container.nodeName
      )
    })

  /**
    * get the container for specify name. Noted: the name should be unique in container env. However, the implementation
    * has no such limit possibly. However, you should NOT assume the implementation and then use the specification based
    * on specify implementation.
    *
    * @param name container name
    * @param executionContext thread pool
    * @return container name
    */
  def containerNames(name: String)(implicit executionContext: ExecutionContext): Future[Seq[ContainerName]] =
    containerNames().map(_.filter(_.name == name))

  /**
    * remove specify container. The container is removed by graceful shutdown.
    * @param name container name
    * @param executionContext thread pool
    * @return empty future if the container is removed successfully.
    */
  def remove(name: String)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * force remove specify container.
    * @param name container name
    * @param executionContext thread pool
    * @return empty future if the container is removed successfully.
    */
  def forceRemove(name: String)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * get container log
    * @param name container name
    * @param executionContext thread pool
    * @return container log
    */
  def log(name: String)(implicit executionContext: ExecutionContext): Future[Map[ContainerName, String]] =
    log(name, None)

  /**
    * get container log
    * @param name container name
    * @param sinceSeconds time windows
    * @param executionContext thread pool
    * @return container log
    */
  def log(name: String, sinceSeconds: Option[Long])(
    implicit executionContext: ExecutionContext
  ): Future[Map[ContainerName, String]]

  /**
    * start a progress to setup a container
    * @return container creator
    */
  def containerCreator: ContainerCreator

  /**
    * @param executionContext thread pool
    * @return all container images on all hosted nodes.
    */
  def imageNames()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[String]]]

  /**
    * @param executionContext thread pool
    * @return all resources on all hosted nodes
    */
  def resources()(implicit executionContext: ExecutionContext): Future[Map[String, Seq[Resource]]]

  /**
    * start a progress to create a named volume on specify node.
    * @return volume creator
    */
  def volumeCreator: VolumeCreator

  /**
    * @param executionContext thread pool
    * @return all ohara volumes across all hosted nodes
    */
  def volumes()(implicit executionContext: ExecutionContext): Future[Seq[ContainerVolume]]

  /**
    * @param executionContext thread pool
    * @return all ohara volumes across all hosted nodes
    */
  def volumes(name: String)(implicit executionContext: ExecutionContext): Future[Seq[ContainerVolume]] =
    volumes().map(_.filter(_.name == name))

  /**
    * remove volumes having same name
    * @param name volume name
    * @return true if there is a deleted volume. Otherwise, false
    */
  def removeVolumes(name: String)(implicit executionContext: ExecutionContext): Future[Unit]
}

object ContainerClient {
  trait VolumeCreator extends oharastream.ohara.common.pattern.Creator[Future[Unit]] {
    private[this] var name: String                                = CommonUtils.randomString()
    private[this] var path: String                                = _
    private[this] var nodeName: String                            = _
    private[this] implicit var executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    @Optional("default is random string")
    def name(name: String): VolumeCreator.this.type = {
      this.name = CommonUtils.requireNonEmpty(name)
      this
    }

    def path(path: String): VolumeCreator.this.type = {
      this.path = CommonUtils.requireNonEmpty(path)
      this
    }

    def nodeName(nodeName: String): VolumeCreator.this.type = {
      this.nodeName = CommonUtils.requireNonEmpty(nodeName)
      this
    }

    @Optional("default pool is scala.concurrent.ExecutionContext.Implicits.global")
    def threadPool(executionContext: ExecutionContext): VolumeCreator.this.type = {
      this.executionContext = Objects.requireNonNull(executionContext)
      this
    }

    final override def create(): Future[Unit] = doCreate(
      nodeName = CommonUtils.requireNonEmpty(nodeName),
      name = CommonUtils.requireNonEmpty(name),
      path = CommonUtils.requireNonEmpty(path),
      executionContext = Objects.requireNonNull(executionContext)
    )

    protected def doCreate(
      nodeName: String,
      name: String,
      path: String,
      executionContext: ExecutionContext
    ): Future[Unit]
  }

  trait ContainerCreator extends oharastream.ohara.common.pattern.Creator[Future[Unit]] {
    private[this] var nodeName: String                            = _
    private[this] var hostname: String                            = CommonUtils.randomString()
    private[this] implicit var executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
    private[this] var imageName: String                           = _
    private[this] var volumeMaps: Map[String, String]             = Map.empty
    private[this] var name: String                                = CommonUtils.randomString()
    private[this] var command: Option[String]                     = None
    private[this] var arguments: Seq[String]                      = Seq.empty
    private[this] var ports: Map[Int, Int]                        = Map.empty
    private[this] var envs: Map[String, String]                   = Map.empty
    private[this] var routes: Map[String, String]                 = Map.empty

    /**
      * set container's name. default is a random string
      *
      * @param name container name
      * @return this builder
      */
    @Optional("default is random string")
    def name(name: String): ContainerCreator.this.type = {
      this.name = CommonUtils.requireNonEmpty(name)
      this
    }

    def nodeName(nodeName: String): ContainerCreator.this.type = {
      this.nodeName = CommonUtils.requireNonEmpty(nodeName)
      this
    }

    /**
      * set target image
      *
      * @param imageName docker image
      * @return this builder
      */
    def imageName(imageName: String): ContainerCreator.this.type = {
      this.imageName = CommonUtils.requireNonEmpty(imageName)
      this
    }

    /**
      * set volume name and mapping container path
      *
      * @param volumeMaps volumn name -> local path
      * @return this builder
      */
    @Optional("default is empty")
    def volumeMaps(volumeMaps: Map[String, String]): ContainerCreator.this.type = {
      this.volumeMaps = volumeMaps
      this
    }

    /**
      * @param hostname the hostname of container
      * @return this builder
      */
    @Optional("default is random string")
    def hostname(hostname: String): ContainerCreator.this.type = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    /**
      * @param envs the env variables exposed to container
      * @return this builder
      */
    @Optional("default is empty")
    def envs(envs: Map[String, String]): ContainerCreator.this.type = {
      this.envs = Objects.requireNonNull(envs)
      this
    }

    /**
      * @param routes the pre-defined route to container. hostname -> ip
      * @return this builder
      */
    @Optional("default is empty")
    def routes(routes: Map[String, String]): ContainerCreator.this.type = {
      this.routes = Objects.requireNonNull(routes)
      this
    }

    /**
      * forward the port from host to container.
      * NOTED: currently we don't support to specify the network interface so the forwarded port is bound on all network adapters.
      *
      * @param ports port mapping (host's port -> container's port)
      * @return this builder
      */
    @Optional("default is empty")
    def portMappings(ports: Map[Int, Int]): ContainerCreator.this.type = {
      this.ports = Objects.requireNonNull(ports)
      this
    }

    /**
      * the arguments passed to docker container
      *
      * @param arguments arguments
      * @return this builder
      */
    @Optional("default is empty")
    def arguments(arguments: Seq[String]): ContainerCreator.this.type = {
      this.arguments = Objects.requireNonNull(arguments)
      this
    }

    /**
      * the command passed to docker container
      *
      * @param command command
      * @return this builder
      */
    @Optional("default is empty")
    def command(command: String): ContainerCreator.this.type = {
      this.command = Some(Objects.requireNonNull(command))
      this
    }

    @Optional("default pool is scala.concurrent.ExecutionContext.Implicits.global")
    def threadPool(executionContext: ExecutionContext): ContainerCreator.this.type = {
      this.executionContext = Objects.requireNonNull(executionContext)
      this
    }

    final override def create(): Future[Unit] = doCreate(
      nodeName = CommonUtils.requireNonEmpty(nodeName),
      hostname = CommonUtils.requireNonEmpty(hostname),
      imageName = CommonUtils.requireNonEmpty(imageName),
      volumeMaps = Objects.requireNonNull(volumeMaps),
      name = CommonUtils.requireNonEmpty(name),
      command = Objects.requireNonNull(command),
      arguments = Objects.requireNonNull(arguments),
      ports = Objects.requireNonNull(ports),
      envs = Objects.requireNonNull(envs),
      routes = Objects.requireNonNull(routes),
      executionContext = Objects.requireNonNull(executionContext)
    )

    /**
      *
      * @param volumeMaps volume name -> local path
      */
    protected def doCreate(
      nodeName: String,
      hostname: String,
      imageName: String,
      volumeMaps: Map[String, String],
      name: String,
      command: Option[String],
      arguments: Seq[String],
      ports: Map[Int, Int],
      envs: Map[String, String],
      routes: Map[String, String],
      executionContext: ExecutionContext
    ): Future[Unit]
  }
}
