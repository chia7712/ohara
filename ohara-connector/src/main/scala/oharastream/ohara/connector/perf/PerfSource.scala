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
import java.util.concurrent.atomic.AtomicInteger

import oharastream.ohara.common.annotations.VisibleForTesting
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.kafka.connector.{RowSourceConnector, RowSourceTask, TaskSetting}

import scala.jdk.CollectionConverters._

class PerfSource extends RowSourceConnector {
  @VisibleForTesting
  private[perf] var settings: TaskSetting = _

  override protected def taskClass(): Class[_ <: RowSourceTask] = classOf[PerfSourceTask]

  override protected def taskSettings(maxTasks: Int): java.util.List[TaskSetting] = Seq.fill(maxTasks)(settings).asJava

  /**
    * this method is exposed to test scope
    */
  override protected def run(settings: TaskSetting): Unit = {
    if (settings.topicKeys().isEmpty) throw new IllegalArgumentException("topics can't be empty")
    val props = PerfSourceProps(settings)
    if (props.batch < 0) throw new IllegalArgumentException(s"batch:${props.batch} can't be negative")
    this.settings = settings
  }

  override protected def terminate(): Unit = {}

  override protected def customSettingDefinitions(): java.util.Map[String, SettingDef] = PerfSource.DEFINITIONS.asJava
}

object PerfSource {
  private[this] val GROUP_COUNT = new AtomicInteger()
  val DEFINITIONS: Map[String, SettingDef] = Seq(
    SettingDef
      .builder()
      .displayName("Batch")
      .documentation("The batch of perf")
      .key(PerfSourceProps.PERF_BATCH_KEY)
      .optional(PerfSourceProps.PERF_BATCH_DEFAULT)
      .orderInGroup(GROUP_COUNT.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("Frequency")
      .documentation("The frequency of perf")
      .key(PerfSourceProps.PERF_FREQUENCY_KEY)
      .optional(java.time.Duration.ofMillis(PerfSourceProps.PERF_FREQUENCY_DEFAULT.toMillis))
      .orderInGroup(GROUP_COUNT.getAndIncrement())
      .build(),
    SettingDef
      .builder()
      .displayName("cell length")
      .documentation("increase this value if you prefer to large cell. Noted, it works only for string type")
      .key(PerfSourceProps.PERF_CELL_LENGTH_KEY)
      .optional(PerfSourceProps.PERF_CELL_LENGTH_DEFAULT)
      .orderInGroup(GROUP_COUNT.getAndIncrement())
      .build()
  ).map(defintion => defintion.key() -> defintion).toMap
}
