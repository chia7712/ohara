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

import oharastream.ohara.agent.NoSuchClusterException
import oharastream.ohara.agent.docker.ContainerState
import oharastream.ohara.client.configurator.ClusterInfo
import oharastream.ohara.client.configurator.ContainerApi.ContainerInfo
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.Timeout

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@Timeout(value = 10, unit = TimeUnit.MINUTES)
trait IntegrationTest {
  protected def result[T](f: Future[T]): T = Await.result(f, Duration(120, TimeUnit.SECONDS))

  /**
    * await until the function return true. If the function fails, the exception is thrown.
    * @param f verification function
    */
  protected def await(f: () => Boolean): Unit = await(f, false)

  /**
    * await until the function return true
    * @param f verification function
    * @param swallowException true if your want to swallow exception and keep running
    */
  protected def await(f: () => Boolean, swallowException: Boolean): Unit =
    CommonUtils.await(
      () =>
        try f()
        catch {
          case _: Throwable if swallowException =>
            false
        },
      java.time.Duration.ofMinutes(2)
    )

  /**
    * the creation of cluster is async so you need to wait the cluster to build.
    * @param clusters clusters
    * @param containers containers
    * @param clusterKey cluster key
    */
  protected def assertCluster(
    clusters: () => Seq[ClusterInfo],
    containers: () => Seq[ContainerInfo],
    clusterKey: ObjectKey
  ): Unit =
    await(
      () =>
        try {
          clusters().exists(_.key == clusterKey) &&
          // since we only get "active" containers, all containers belong to the cluster should be running.
          // Currently, both k8s and pure docker have the same context of "RUNNING".
          // It is ok to filter container via RUNNING state.
          containers().nonEmpty &&
          containers().map(_.state).forall(_.equals(ContainerState.RUNNING.name))
        } catch {
          // the collie impl throw exception if the cluster "does not" exist when calling "containers"
          case _: NoSuchClusterException => false
        }
    )

  /**
    * used to release object after evaluation is done.
    * @param obj releasable object
    * @param f evaluation function
    * @param cleanup a call in the final (the error is swallowed)
    * @tparam T type of object
    * @tparam R type of response
    * @return response created by function
    */
  protected def close[T <: AutoCloseable, R](obj: T)(f: T => R)(cleanup: T => Unit): R =
    try f(obj)
    finally try Releasable.close(() => cleanup(obj))
    finally obj.close()
}
