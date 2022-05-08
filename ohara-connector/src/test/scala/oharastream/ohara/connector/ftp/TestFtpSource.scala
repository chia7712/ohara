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

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.connector.CsvSourceTestBase
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.{
  COMPLETED_FOLDER_KEY,
  ERROR_FOLDER_KEY,
  INPUT_FOLDER_KEY
}
import oharastream.ohara.kafka.connector.csv.CsvSourceConnector

class TestFtpSource extends CsvSourceTestBase {
  private[this] val ftpServer = testUtil.ftpServer

  override val fileSystem: FileSystem =
    FileSystem.ftpBuilder
      .hostname(ftpServer.hostname)
      .port(ftpServer.port)
      .user(ftpServer.user)
      .password(ftpServer.password)
      .build

  override val connectorClass: Class[_ <: CsvSourceConnector] = classOf[FtpSource]

  override val props: Map[String, String] =
    Map(
      FTP_HOSTNAME_KEY     -> ftpServer.hostname,
      FTP_PORT_KEY         -> ftpServer.port.toString,
      FTP_USER_NAME_KEY    -> ftpServer.user,
      FTP_PASSWORD_KEY     -> ftpServer.password,
      INPUT_FOLDER_KEY     -> s"/input${CommonUtils.randomString(5)}",
      COMPLETED_FOLDER_KEY -> s"/completed${CommonUtils.randomString(5)}",
      ERROR_FOLDER_KEY     -> s"/error${CommonUtils.randomString(5)}"
    )
}
