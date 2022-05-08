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

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.MetricsApi.{Meter, Metrics}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.configurator.store.MetricsCache.RequestKey
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.Duration

class TestMetricsCache extends OharaTest {
  @Test
  def testRequestKey(): Unit = {
    val key = RequestKey(
      key = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString()),
      service = CommonUtils.randomString()
    )

    key shouldBe key
    key should not be key.copy(key = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString()))
    key should not be key.copy(service = CommonUtils.randomString())
  }

  @Test
  def nullRefresher(): Unit =
    an[NullPointerException] should be thrownBy MetricsCache.builder.refresher(null)

  @Test
  def nullFrequency(): Unit =
    an[NullPointerException] should be thrownBy MetricsCache.builder.frequency(null)

  @Test
  def testRefresh(): Unit = {
    val data = Map(
      ObjectKey.of("a", "b") -> Metrics(
        Seq(
          Meter(
            name = "name",
            value = 1.1,
            unit = "unit",
            document = "document",
            queryTime = CommonUtils.current(),
            startTime = Some(CommonUtils.current()),
            lastModified = Some(CommonUtils.current()),
            valueInPerSec = None
          )
        )
      )
    )
    val clusterInfo = FakeClusterInfo(CommonUtils.randomString())
    val cache = MetricsCache.builder
      .refresher(() => Map(clusterInfo -> Map(CommonUtils.hostname() -> data)))
      .frequency(Duration(2, TimeUnit.SECONDS))
      .build
    try {
      cache.meters(clusterInfo) shouldBe Map.empty
      TimeUnit.SECONDS.sleep(3)
      cache.meters(clusterInfo)(CommonUtils.hostname()) shouldBe data
    } finally cache.close()
  }

  @Test
  def failToOperateAfterClose(): Unit = {
    val cache = MetricsCache.builder.refresher(() => Map.empty).frequency(Duration(2, TimeUnit.SECONDS)).build
    cache.close()

    an[IllegalStateException] should be thrownBy cache.meters(FakeClusterInfo(CommonUtils.randomString()))
  }
}
