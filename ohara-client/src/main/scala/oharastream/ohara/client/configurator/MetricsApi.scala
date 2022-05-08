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

import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

object MetricsApi {
  /**
    * the metric information
    * @param name the name of this meter
    * @param value the value of metric record
    * @param unit the unit of metric record
    * @param document the document of metric record
    * @param queryTime the time of query metrics object
    * @param startTime the time of record generated in remote machine
    */
  final case class Meter(
    name: String,
    value: Double,
    valueInPerSec: Option[Double],
    unit: String,
    document: String,
    queryTime: Long,
    startTime: Option[Long],
    lastModified: Option[Long]
  ) {
    def duration: Option[Long] = lastModified.flatMap(last => startTime.map(s => last - s))
  }

  implicit val METER_FORMAT: RootJsonFormat[Meter] = jsonFormat8(Meter)
  final case class Metrics(meters: Seq[Meter])
  object Metrics {
    val EMPTY: Metrics = Metrics(Seq.empty)
  }
  implicit val METRICS_FORMAT: RootJsonFormat[Metrics] = jsonFormat1(Metrics.apply)
}
