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

package oharastream.ohara.connector.ftp

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.setting.SettingDef.{Necessary, Permission, Reference}
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestFtpSinkDefinitions extends OharaTest {
  private[this] val ftpSink = new FtpSink
  @Test
  def checkOutputFolder(): Unit = {
    val definition = ftpSink.settingDefinitions().get(OUTPUT_FOLDER_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkNeedHeader(): Unit = {
    val definition = ftpSink.settingDefinitions().get(FILE_NEED_HEADER_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultBoolean() shouldBe true
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.BOOLEAN
  }

  @Test
  def checkEncode(): Unit = {
    val definition = ftpSink.settingDefinitions().get(FILE_ENCODE_KEY)
    definition.necessary() should not be Necessary.REQUIRED
    definition.defaultString() shouldBe "UTF-8"
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkHostname(): Unit = {
    val definition = ftpSink.settingDefinitions().get(FTP_HOSTNAME_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkPort(): Unit = {
    val definition = ftpSink.settingDefinitions().get(FTP_PORT_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.REMOTE_PORT
  }

  @Test
  def checkUser(): Unit = {
    val definition = ftpSink.settingDefinitions().get(FTP_USER_NAME_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.STRING
  }

  @Test
  def checkPassword(): Unit = {
    val definition = ftpSink.settingDefinitions().get(FTP_PASSWORD_KEY)
    definition.necessary() shouldBe Necessary.REQUIRED
    definition.hasDefault shouldBe false
    definition.permission() shouldBe Permission.EDITABLE
    definition.internal() shouldBe false
    definition.reference() shouldBe Reference.NONE
    definition.valueType() shouldBe SettingDef.Type.PASSWORD
  }
}
