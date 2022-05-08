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

package oharastream.ohara.configurator.route

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.setting.SettingDef.Permission
import org.junit.jupiter.api.Test
import spray.json.JsString
import org.scalatest.matchers.should.Matchers._

class TestRouteUtils extends OharaTest {
  @Test
  def testUpdatable(): Unit = {
    val settings = Map("a" -> JsString("b"))
    val settingDef = SettingDef
      .builder()
      .key("a")
      .permission(Permission.CREATE_ONLY)
      .build()
    keepEditableFields(settings, Seq(settingDef)) shouldBe Map.empty
  }
}
