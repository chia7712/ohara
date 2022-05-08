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

package oharastream.ohara.connector.smb

import oharastream.ohara.kafka.connector.TaskSetting

case class SmbProps(hostname: String, port: Int, user: String, password: String, shareName: String) {
  def plain: Map[String, String] = Map(
    SMB_HOSTNAME_KEY   -> hostname,
    SMB_PORT_KEY       -> port.toString,
    SMB_USER_KEY       -> user,
    SMB_PASSWORD_KEY   -> password,
    SMB_SHARE_NAME_KEY -> shareName
  )
}

object SmbProps {
  def apply(settings: TaskSetting): SmbProps = SmbProps(
    hostname = settings.stringValue(SMB_HOSTNAME_KEY),
    port = settings.intValue(SMB_PORT_KEY),
    user = settings.stringValue(SMB_USER_KEY),
    password = settings.stringValue(SMB_PASSWORD_KEY),
    shareName = settings.stringValue(SMB_SHARE_NAME_KEY)
  )
}
