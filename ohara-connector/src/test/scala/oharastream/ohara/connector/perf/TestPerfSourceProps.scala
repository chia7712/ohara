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

package oharastream.ohara.connector.perf
import java.util.concurrent.TimeUnit

import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.TaskSetting
import oharastream.ohara.kafka.connector.json.ConnectorFormatter
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class TestPerfSourceProps extends OharaTest {
  private[this] val props     = PerfSourceProps(10, Duration(20, TimeUnit.SECONDS), 10)
  private[this] val topicKeys = Set(TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
  private[this] val schema    = Seq(Column.builder().name("name").dataType(DataType.SHORT).order(1).build())

  @Test
  def testPlainMap(): Unit = {
    val props = PerfSourceProps(123, Duration(20, TimeUnit.SECONDS), 10)
    val copy  = PerfSourceProps(TaskSetting.of(props.toMap.asJava))
    props shouldBe copy
  }

  @Test
  def testEmptyTopics(): Unit = {
    val source = new PerfSource

    an[NoSuchElementException] should be thrownBy source.start(
      ConnectorFormatter
        .of()
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .columns(schema.asJava)
        .settings(props.toMap.asJava)
        .raw()
    )
  }

  @Test
  def testEmptySchemaOnSource(): Unit = {
    val source = new PerfSource

    source.start(
      ConnectorFormatter
        .of()
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .topicKeys(topicKeys.asJava)
        .settings(props.toMap.asJava)
        .raw()
    )
  }

  @Test
  def testEmptySchemaOnSourceTask(): Unit = {
    val task = new PerfSourceTask

    task.start(
      ConnectorFormatter
        .of()
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .topicKeys(topicKeys.asJava)
        .settings(props.toMap.asJava)
        .raw()
    )

    task.schema shouldBe PerfSourceTask.DEFAULT_SCHEMA
  }

  @Test
  def testDefaultBatch(): Unit =
    PerfSourceProps(TaskSetting.of(java.util.Map.of())).batch shouldBe PerfSourceProps.PERF_BATCH_DEFAULT

  @Test
  def testDefaultFrequency(): Unit =
    PerfSourceProps(TaskSetting.of(java.util.Map.of())).freq shouldBe PerfSourceProps.PERF_FREQUENCY_DEFAULT

  @Test
  def testDefaultCellSize(): Unit =
    PerfSourceProps(TaskSetting.of(java.util.Map.of())).cellSize shouldBe PerfSourceProps.PERF_CELL_LENGTH_DEFAULT

  @Test
  def testCellSize(): Unit =
    PerfSourceProps(TaskSetting.of(java.util.Map.of(PerfSourceProps.PERF_CELL_LENGTH_KEY, "999"))).cellSize shouldBe 999

  @Test
  def testInvalidFrequency(): Unit =
    an[NumberFormatException] should be thrownBy PerfSourceProps(
      TaskSetting.of(Map(PerfSourceProps.PERF_BATCH_KEY -> "1", PerfSourceProps.PERF_FREQUENCY_KEY -> "abc").asJava)
    )

  @Test
  def testInvalidProps(): Unit = {
    val source = new PerfSource
    an[IllegalArgumentException] should be thrownBy source.start(
      ConnectorFormatter
        .of()
        .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
        .topicKeys(topicKeys.asJava)
        .columns(schema.asJava)
        .settings(props.copy(batch = -1).toMap.asJava)
        .raw()
    )
  }
}
