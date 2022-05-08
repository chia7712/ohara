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

package oharastream.ohara.configurator.store

import java.util.Objects
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.ClusterInfo
import oharastream.ohara.client.configurator.MetricsApi.Metrics
import oharastream.ohara.client.configurator.ShabondiApi.ShabondiClusterInfo
import oharastream.ohara.client.configurator.StreamApi.StreamClusterInfo
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.configurator.ZookeeperApi.ZookeeperClusterInfo
import oharastream.ohara.common.annotations.{Optional, VisibleForTesting}
import oharastream.ohara.common.cache.RefreshableCache
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.Releasable

import scala.concurrent.duration.Duration

trait MetricsCache extends Releasable {
  /**
    * get the metrics of specify cluster
    * @param clusterInfo cluster
    * @return node -> object key -> meters
    */
  def meters(clusterInfo: ClusterInfo): Map[String, Map[ObjectKey, Metrics]]

  /**
    * get the metrics of specify object from specify cluster
    * @param clusterInfo cluster
    * @return node -> meters
    */
  def meters(clusterInfo: ClusterInfo, key: ObjectKey): Map[String, Metrics] =
    meters(clusterInfo)
      .map {
        case (hostname, keyAndMeters) =>
          hostname -> keyAndMeters.getOrElse(key, Metrics.EMPTY)
      }
}

object MetricsCache {
  def builder: Builder = new Builder()

  // TODO: remove this workaround if google guava support the custom comparison ... by chia
  @VisibleForTesting
  private[store] case class RequestKey(key: ObjectKey, service: String) {
    override def equals(obj: Any): Boolean = obj match {
      case another: RequestKey => another.key == key && another.service == service
      case _                   => false
    }
    override def hashCode(): Int  = 31 * key.hashCode + service.hashCode
    override def toString: String = s"key:$key, service:$service"
  }

  class Builder private[MetricsCache] extends oharastream.ohara.common.pattern.Builder[MetricsCache] {
    private[this] var refresher: () => Map[ClusterInfo, Map[String, Map[ObjectKey, Metrics]]] = _
    private[this] var frequency: Duration                                                     = Duration(5, TimeUnit.SECONDS)

    def refresher(refresher: () => Map[ClusterInfo, Map[String, Map[ObjectKey, Metrics]]]): Builder = {
      this.refresher = Objects.requireNonNull(refresher)
      this
    }

    @Optional("default value is equal to timeout")
    def frequency(frequency: Duration): Builder = {
      this.frequency = Objects.requireNonNull(frequency)
      this
    }

    override def build: MetricsCache = new MetricsCache {
      import scala.jdk.CollectionConverters._
      private[this] val refresher = Objects.requireNonNull(Builder.this.refresher)
      private[this] val closed    = new AtomicBoolean(false)
      private[this] val cache = RefreshableCache
        .builder[RequestKey, Map[String, Map[ObjectKey, Metrics]]]()
        .supplier(
          () =>
            refresher().map {
              case (clusterInfo, meters) =>
                key(clusterInfo) -> meters
            }.asJava
        )
        .frequency(java.time.Duration.ofMillis(frequency.toMillis))
        .build()

      private[this] def key(clusterInfo: ClusterInfo): RequestKey = RequestKey(
        key = clusterInfo.key,
        service = clusterInfo match {
          case _: ZookeeperClusterInfo => "zk"
          case _: BrokerClusterInfo    => "bk"
          case _: WorkerClusterInfo    => "wk"
          case _: StreamClusterInfo    => "stream"
          case _: ShabondiClusterInfo  => "shabondi"
          case c: ClusterInfo          => c.getClass.getSimpleName // used by testing
        }
      )

      override def meters(clusterInfo: ClusterInfo): Map[String, Map[ObjectKey, Metrics]] =
        cache.get(key(clusterInfo)).orElse(Map.empty)

      override def close(): Unit = if (closed.compareAndSet(false, true)) Releasable.close(cache)
    }
  }
}
