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

package oharastream.ohara.it.connector.smb

import oharastream.ohara.client.filesystem.{FileSystem, FileSystemTestBase}
import oharastream.ohara.common.util.CommonUtils

class TestSmbFileSystem extends FileSystemTestBase with SmbEnv {
  override protected val fileSystem: FileSystem = FileSystem.smbBuilder
    .hostname(hostname)
    .port(port)
    .user(username)
    .password(password)
    .shareName(shareName)
    .build()

  override protected val rootDir: String = CommonUtils.randomString(10)
}
