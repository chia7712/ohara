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

import oharastream.ohara.common.data.DataType
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ConnectorKey, TopicKey}
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestPerfSourceTask extends OharaTest {
  @Test
  def byteArrayShouldBeReused(): Unit = {
    val task = new PerfSourceTask
    task.start(
      java.util.Map.of(
        ConnectorDefUtils.CONNECTOR_KEY_DEFINITION.key,
        "{\"group\": \"g\", \"name\": \"n\"}",
        ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key,
        "[{\"group\": \"g\", \"name\": \"n\"}]",
        PerfSourceProps.PERF_FREQUENCY_KEY,
        "10 milliseconds"
      )
    )
    val records_0 = task.poll()
    java.util.concurrent.TimeUnit.SECONDS.sleep(1)
    val records_1 = task.poll()
    records_0.size() should not be 0
    records_0.size() shouldBe records_1.size()
    (0 until records_0.size()).foreach { index =>
      val row_0 = records_0.get(index).key()
      val row_1 = records_1.get(index).key()
      row_0.isInstanceOf[Array[Byte]] shouldBe true
      row_1.isInstanceOf[Array[Byte]] shouldBe true
      row_0.asInstanceOf[Array[Byte]].hashCode() shouldBe row_1.asInstanceOf[Array[Byte]].hashCode()
    }
  }

  @Test
  def testConvertToValue(): Unit = {
    val perfSourceTask = new PerfSourceTask()
    val connectorKey   = ConnectorKey.of(CommonUtils.randomString(5), "fake-connector-key")
    val topicKey       = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))
    perfSourceTask.start(
      Map(
        PerfSourceProps.PERF_CELL_LENGTH_KEY -> "5",
        "connectorKey"                       -> connectorKey.toString,
        "topicKeys"                          -> s"[$topicKey]"
      ).asJava
    )
    Seq(
      perfSourceTask.convertToValue(DataType.BOOLEAN, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.BYTE, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.SHORT, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.INT, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.LONG, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.FLOAT, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.DOUBLE, CommonUtils.current()),
      perfSourceTask.convertToValue(DataType.STRING, CommonUtils.current())
    ).foreach(_.getClass.getName.startsWith("java.lang") shouldBe true)
    perfSourceTask
      .convertToValue(DataType.BYTES, CommonUtils.current())
      .asInstanceOf[Array[java.lang.Byte]]
      .foreach { x =>
        x.getClass.getName.startsWith("java.lang") shouldBe true
      }
  }
}
