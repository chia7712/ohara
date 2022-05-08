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

package oharastream.ohara.it.code

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.it.connector.{IncludeAllTypesSinkConnector, IncludeAllTypesSourceConnector}
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

/**
  * our QA, by default, have specific plan for all IT so this test case is moved to ohara-assembly.
  */
class TestSettingDefinitionsOfDumbConnector extends OharaTest {
  @Test
  def allTypesShouldBeIncludedByDumbSource(): Unit =
    verify((new IncludeAllTypesSourceConnector).settingDefinitions().values().asScala.toSeq)

  @Test
  def allTypesShouldBeIncludedByDumbSink(): Unit =
    verify((new IncludeAllTypesSinkConnector).settingDefinitions().values().asScala.toSeq)

  private[this] def verify(settingDefs: Seq[SettingDef]): Unit =
    SettingDef.Type.values().foreach(t => settingDefs.map(_.valueType()).toSet should contain(t))
}
