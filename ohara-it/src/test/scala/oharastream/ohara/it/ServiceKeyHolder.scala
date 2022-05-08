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

package oharastream.ohara.it

import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * In agent tests we build many clusters to test. If we don't cleanup them, the resources of it nodes may be exhausted
  * and then sequential tests will be timeout.
  *
  */
trait ServiceKeyHolder extends Releasable {
  /**
    * store the name used to create cluster. We can remove all created cluster in the "after" phase.
    */
  private[this] val usedClusterKeys: mutable.HashSet[ObjectKey] = new mutable.HashSet[ObjectKey]()

  /**
    * our IT env is flooded with many running/exited containers. As a normal human, it is hard to observer the containers
    * invoked by our IT. Hence, this env variable enable us to add prefix to containers.
    */
  private[this] val prefix: String = sys.env.getOrElse("ohara.it.container.prefix", "cnh")

  /**
    * generate a random cluster key. The key is logged and this holder will iterate all nodes to remove all related
    * containers
    *
    * Noted: the group of key is always "default".
    * @return a random key
    */
  def generateObjectKey(): ObjectKey = {
    val key = ObjectKey.of(oharastream.ohara.client.configurator.GROUP_DEFAULT, prefix + CommonUtils.randomString(7))
    usedClusterKeys += key
    key
  }

  override def close(): Unit = release(
    clusterKeys = usedClusterKeys.toSet,
    excludedNodes = Set.empty
  )

  /**
    * remove all containers belonging to input clusters. The argument "excludedNodes" enable you to remove a part of
    * containers from input clusters.
    * @param clusterKeys clusters to remove
    * @param excludedNodes nodes to keep the containers
    */
  protected def release(clusterKeys: Set[ObjectKey], excludedNodes: Set[String]): Unit
}

object ServiceKeyHolder {
  /**
    * used to debug :)
    */
  private[this] val KEEP_CONTAINERS = sys.env.get("ohara.it.keep.containers").exists(_.toLowerCase == "true")
  private[this] val LOG             = Logger(classOf[ServiceKeyHolder])

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(60, TimeUnit.SECONDS))

  /**
    * create a name holder based on k8s.
    * @param client k8s client
    * @return name holder
    */
  def apply(client: ContainerClient): ServiceKeyHolder =
    (clusterKey: Set[ObjectKey], excludedNodes: Set[String]) =>
      try
      /**
        * Some IT need to close containers so we don't obstruct them form "releasing".
        * However, the final close is still controlled by the global flag.
        */
      if (!KEEP_CONTAINERS)
        result(client.containers())
          .filter(
            container =>
              clusterKey.exists(key => container.name.contains(key.group()) && container.name.contains(key.name()))
          )
          .filterNot(container => excludedNodes.contains(container.nodeName))
          .foreach { container =>
            try {
              println(s"[-----------------------------------${container.name}-----------------------------------]")
              // Before 10 minutes container log. Avoid the OutOfMemory of Java heap
              val containerLogs = try result(client.log(container.name, Option(600)))
              catch {
                case e: Throwable =>
                  s"failed to fetch the logs for container:${container.name}. caused by:${e.getMessage}"
              }
              println(containerLogs)
              println("[------------------------------------------------------------------------------------]")
              result(client.forceRemove(container.name))
            } catch {
              case e: Throwable =>
                LOG.error(s"failed to remove container ${container.name}", e)
            }
          } finally Releasable.close(client)
}
