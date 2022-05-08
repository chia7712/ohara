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

package oharastream.ohara.connector.validation

import java.util

import oharastream.ohara.client.configurator.InspectApi
import oharastream.ohara.common.util.VersionUtils
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.jdk.CollectionConverters._

/**
  * This class is used to verify the connection to 1) HDFS, 2) KAFKA and 3) RDB. Since ohara have many sources/sinks implemented
  * by kafak connector, the verification from connection should be run at the worker nodes. This class is implemented by kafka
  * souce connector in order to run the validation on all worker nodes.
  * TODO: refactor this one...it is ugly I'd say... by chia
  */
class Validator extends SourceConnector {
  private[this] var props: util.Map[String, String] = _
  override def version(): String                    = VersionUtils.VERSION
  override def start(props: util.Map[String, String]): Unit = {
    this.props = new util.HashMap[String, String](props)
    // we don't want to make any exception here
  }

  override def taskClass(): Class[_ <: Task] = classOf[ValidatorTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] =
    Seq.fill(maxTasks)(new util.HashMap[String, String](props)).map(_.asInstanceOf[util.Map[String, String]]).asJava

  override def stop(): Unit = {
    // do nothing
  }
  override def config(): ConfigDef =
    new ConfigDef().define(InspectApi.TARGET_KEY, Type.STRING, null, Importance.HIGH, "target type")
}
