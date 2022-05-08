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

package oharastream.ohara.it.performance

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ConnectorKey
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestPerformanceReport extends OharaTest {
  private[this] val groupName = "benchmark"

  @Test
  def testCleanValue(): Unit = {
    val headerName                        = "header1"
    val report: PerformanceReport.Builder = PerformanceReport.builder
    val record = report
      .connectorKey(ConnectorKey.of(groupName, CommonUtils.randomString(5)))
      .className("class")
      .record(1, headerName, 100)
      .build
    // Before clean value
    record.records(1)(headerName) shouldBe 100

    // After clean value
    report
      .resetValue(1, headerName)
      .build
      .records(1)(headerName) shouldBe 0.0
  }

  @Test
  def testRecord1(): Unit = {
    val headerName                        = "header1"
    val report: PerformanceReport.Builder = PerformanceReport.builder
    report
      .connectorKey(ConnectorKey.of(groupName, CommonUtils.randomString(5)))
      .className("class")
      .record(1, headerName, 100)
      .record(1, headerName, 200)
      .record(1, headerName, 300)
      .build
      .records(1)(headerName) shouldBe 600
  }

  @Test
  def testRecord2(): Unit = {
    val headerName                        = "header1"
    val report: PerformanceReport.Builder = PerformanceReport.builder
    report
      .connectorKey(ConnectorKey.of(groupName, CommonUtils.randomString(5)))
      .className("class")
      .resetValue(1, headerName)
      .record(1, headerName, 100)
      .record(1, headerName, 200)
      .record(1, headerName, 300)
      .resetValue(1, headerName)
      .record(1, headerName, 100)
      .record(1, headerName, 200)
      .record(1, headerName, 300)
      .build
      .records(1)(headerName) shouldBe 600
  }
}
