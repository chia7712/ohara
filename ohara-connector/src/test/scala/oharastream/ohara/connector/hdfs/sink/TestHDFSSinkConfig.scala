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

package oharastream.ohara.connector.hdfs.sink

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.kafka.connector.TaskSetting
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class TestHDFSSinkConfig extends OharaTest {
  private[this] val HDFS_URL_VALUE = "hdfs://test:9000"

  private[this] def hdfsConfig(settings: Map[String, String]): HDFSSinkProps =
    HDFSSinkProps(TaskSetting.of(settings.asJava))

  @Test
  def testGetDataDir(): Unit = {
    val hdfsSinkConfig: HDFSSinkProps = hdfsConfig(Map(HDFS_URL_KEY -> HDFS_URL_VALUE))
    hdfsSinkConfig.hdfsURL shouldBe HDFS_URL_VALUE
  }

  @Test
  def testReplication(): Unit = {
    val hdfsSinkConfig: HDFSSinkProps = hdfsConfig(
      Map(HDFS_URL_KEY -> HDFS_URL_VALUE, HDFS_REPLICATION_NUMBER_KEY -> "2")
    )
    hdfsSinkConfig.replicationNumber shouldBe 2
  }
}
