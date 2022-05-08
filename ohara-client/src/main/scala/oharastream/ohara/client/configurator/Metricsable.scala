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

package oharastream.ohara.client.configurator

import oharastream.ohara.client.configurator.MetricsApi.{Meter, Metrics}

/**
  * a interface offering the metrics information.
  *
  * there are a lot of objects in ohara and some of them are able to provide metrics. Those metrics are collected from
  * cluster (i.e multiples alive nodes).
  */
trait Metricsable {
  def nodeMetrics: Map[String, Metrics]

  /**
    * collect all meters from all alive nodes.
    * @return meters from all alive nodes
    */
  def meters: Seq[Meter] = nodeMetrics.values.flatMap(_.meters).toSeq
}
