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

import java.util.Objects
import java.util.concurrent.TimeUnit

import oharastream.ohara.agent.{ClusterKind, ClusterStatus}
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.cache.RefreshableCache
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/**
  * The cost of fetching containers via ssh is too expensive, and our ssh collie almost die for it. A quick solution
  * is a simple cache which storing the cluster information in local memory. The cache shines at getter method however
  * the side-effect is that the stuff from getter may be out-of-date. But, in most use cases we bear it well.
  *
  * Noted that the comparison of key (ClusterStatus) consists of only ObjectKey and service type. Adding a cluster info having different node names
  * does not create an new key-value pair. Also, the latter key will replace the older one. For example, adding a cluster info having different
  * port. will replace the older cluster info, and then you will get the new cluster info when calling snapshot method.
  */
trait ServiceCache extends Releasable {
  /**
    * @return the cached data
    */
  def snapshot: Seq[ClusterStatus]

  /**
    * The inner time-based auto-refresher is enough to most use cases. However, we are always in a situation that we should
    * update the cache right now. This method save your life that you can request the inner thread to update the cache.
    * Noted, the method doesn't block your thread since what it does is to send a request without any wait.
    */
  def requestUpdate(): Unit

  /**
    * add data to cache.
    * Noted, the key(ClusterStatus) you added will replace the older one in calling this method..
    *
    * @param clusterStatus cluster info
    */
  def put(clusterStatus: ClusterStatus): Unit

  /**
    * remove the cached cluster data
    * @param clusterStatus cluster info
    */
  def remove(clusterStatus: ClusterStatus): Unit
}

object ServiceCache {
  def builder: Builder = new Builder

  // TODO: remove this workaround if google guava support the custom comparison ... by chia
  @VisibleForTesting
  private[agent] case class RequestKey(key: ObjectKey, kind: ClusterKind, createdTime: Long) {
    override def equals(obj: Any): Boolean = obj match {
      case another: RequestKey => another.key == key && another.kind == kind
      case _                   => false
    }
    override def hashCode(): Int = 31 * key.hashCode + kind.hashCode

    override def toString: String = s"name:${ObjectKey.toJsonString(key)}, kind:$kind"
  }

  class Builder private[ServiceCache] extends oharastream.ohara.common.pattern.Builder[ServiceCache] {
    private[this] var frequency: Duration                = Duration(5, TimeUnit.SECONDS)
    private[this] var lazyRemove: Duration               = Duration(0, TimeUnit.SECONDS)
    private[this] var supplier: () => Seq[ClusterStatus] = _

    @Optional("default value is 5 seconds")
    def frequency(frequency: Duration): Builder = {
      this.frequency = Objects.requireNonNull(frequency)
      this
    }

    /**
      * this is a workaround to avoid cache from removing a cluster which is just added.
      * Ssh collie mocks all cluster information when user change the cluster in order to speed up the following operations.
      * however, the cache thread may remove the mocked cluster from cache since it doesn't see the associated containers from remote nodes.
      * @param lazyRemove the time to do remove data from cache
      * @return this builder
      */
    @Optional("default value is 0 seconds")
    def lazyRemove(lazyRemove: Duration): Builder = {
      this.lazyRemove = Objects.requireNonNull(lazyRemove)
      this
    }

    /**
      * the function to supply all data to cache.
      * @param supplier supplier
      * @return this builder
      */
    def supplier(supplier: () => Seq[ClusterStatus]): Builder = {
      this.supplier = Objects.requireNonNull(supplier)
      this
    }

    private[this] def checkArguments(): Unit = {
      Objects.requireNonNull(frequency)
      Objects.requireNonNull(supplier)
    }

    override def build: ServiceCache = {
      checkArguments()
      new ServiceCache {
        private[this] val cache = RefreshableCache
          .builder[RequestKey, ClusterStatus]()
          .supplier(
            () => supplier().map(clusterStatus => key(clusterStatus) -> clusterStatus).toMap.asJava
          )
          .frequency(java.time.Duration.ofMillis(frequency.toMillis))
          .preRemoveObserver((key, _) => CommonUtils.current() - key.createdTime > lazyRemove.toMillis)
          .build()

        override def close(): Unit = Releasable.close(cache)

        override def snapshot: Seq[ClusterStatus] = cache.snapshot().asScala.values.toSeq

        override def requestUpdate(): Unit = cache.requestUpdate()

        private def key(clusterStatus: ClusterStatus): RequestKey = RequestKey(
          key = clusterStatus.key,
          kind = clusterStatus.kind,
          createdTime = CommonUtils.current()
        )

        override def put(clusterStatus: ClusterStatus): Unit =
          cache.put(key(clusterStatus), clusterStatus)

        override def remove(clusterStatus: ClusterStatus): Unit =
          cache.remove(key(clusterStatus))
      }
    }
  }
}
