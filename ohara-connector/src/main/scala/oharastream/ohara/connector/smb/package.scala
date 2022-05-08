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

package oharastream.ohara.connector

import java.util.concurrent.atomic.AtomicInteger

import oharastream.ohara.common.setting.SettingDef

package object smb {
  /**
    * used to set the order of definitions.
    */
  private[this] val COUNTER    = new AtomicInteger(0)
  val SMB_HOSTNAME_KEY: String = "smb.hostname"
  val SMB_HOSTNAME_DEFINITION: SettingDef = SettingDef
    .builder()
    .key(SMB_HOSTNAME_KEY)
    .displayName("Hostname")
    .documentation("the hostname of SMB server")
    .required(SettingDef.Type.STRING)
    .orderInGroup(COUNTER.getAndIncrement())
    .build()

  val SMB_PORT_KEY: String = "smb.port"
  val SMB_PORT_DEFINITION: SettingDef = SettingDef
    .builder()
    .key(SMB_PORT_KEY)
    .displayName("Port")
    .documentation("the port of SMB server")
    .required(SettingDef.Type.REMOTE_PORT)
    .orderInGroup(COUNTER.getAndIncrement())
    .build()

  val SMB_USER_KEY: String = "smb.user"
  val SMB_USER_DEFINITION: SettingDef = SettingDef
    .builder()
    .key(SMB_USER_KEY)
    .displayName("Username")
    .documentation(
      "the username of SMB server. This account must have read/delete permission of input folder and error folder"
    )
    .required(SettingDef.Type.STRING)
    .orderInGroup(COUNTER.getAndIncrement())
    .build()

  val SMB_PASSWORD_KEY: String = "smb.password"
  val SMB_PASSWORD_DEFINITION: SettingDef = SettingDef
    .builder()
    .key(SMB_PASSWORD_KEY)
    .displayName("Password")
    .documentation("the password of SMB server")
    .required(SettingDef.Type.PASSWORD)
    .orderInGroup(COUNTER.getAndIncrement())
    .build()

  val SMB_SHARE_NAME_KEY: String = "smb.shareName"
  val SMB_SHARE_NAME_DEFINITION: SettingDef = SettingDef
    .builder()
    .key(SMB_SHARE_NAME_KEY)
    .displayName("Share Name")
    .documentation("the share name of SMB server")
    .required(SettingDef.Type.STRING)
    .orderInGroup(COUNTER.getAndIncrement())
    .build()

  /**
    * the settings for smb connectors.
    */
  val DEFINITIONS = Map(
    SMB_HOSTNAME_DEFINITION.key()   -> SMB_HOSTNAME_DEFINITION,
    SMB_PORT_DEFINITION.key()       -> SMB_PORT_DEFINITION,
    SMB_USER_DEFINITION.key()       -> SMB_USER_DEFINITION,
    SMB_PASSWORD_DEFINITION.key()   -> SMB_PASSWORD_DEFINITION,
    SMB_SHARE_NAME_DEFINITION.key() -> SMB_SHARE_NAME_DEFINITION
  )
}
