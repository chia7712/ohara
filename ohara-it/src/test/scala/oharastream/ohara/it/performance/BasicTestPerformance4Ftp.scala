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

package oharastream.ohara.it.performance

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.atomic.LongAdder

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.data.Row
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import spray.json.{JsNumber, JsString, JsValue}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

@EnabledIfEnvironmentVariable(named = "ohara.it.performance.ftp.hostname", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.ftp.port", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.ftp.user", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.ftp.password", matches = ".*")
private[performance] abstract class BasicTestPerformance4Ftp extends BasicTestPerformance {
  private[this] val FTP_HOSTNAME_KEY: String = "ohara.it.performance.ftp.hostname"
  private[this] val FTP_PORT_KEY: String     = "ohara.it.performance.ftp.port"
  private[this] val FTP_USER_KEY: String     = "ohara.it.performance.ftp.user"
  private[this] val FTP_PASSWORD_KEY: String = "ohara.it.performance.ftp.password"

  private[this] val ftpHostname = sys.env(FTP_HOSTNAME_KEY)

  private[this] val ftpPort = sys.env(FTP_PORT_KEY).toInt

  private[this] val ftpUser = sys.env(FTP_USER_KEY)

  private[this] val ftpPassword = sys.env(FTP_PASSWORD_KEY)

  /**
    * add the route for ftp hostname to avoid the hostname error from remote services...
    * @return routes routes added to all services
    */
  override def routes: Map[String, String] = Map(ftpHostname -> CommonUtils.address(ftpHostname))

  /**
    * generate the default settings according to the ftp server. It includes
    * 1) hostname
    * 2) port
    * 3) user name
    * 4) user password
    */
  protected val ftpSettings: Map[String, JsValue] = Map(
    // convert the hostname to IP address
    oharastream.ohara.connector.ftp.FTP_HOSTNAME_KEY  -> JsString(ftpHostname),
    oharastream.ohara.connector.ftp.FTP_PORT_KEY      -> JsNumber(ftpPort),
    oharastream.ohara.connector.ftp.FTP_USER_NAME_KEY -> JsString(ftpUser),
    oharastream.ohara.connector.ftp.FTP_PASSWORD_KEY  -> JsString(ftpPassword)
  )

  private[this] val csvInputFolderKey       = PerformanceTestingUtils.CSV_INPUT_KEY
  private[this] val csvOutputFolder: String = value(csvInputFolderKey).getOrElse("/input")

  private[this] val cleanupTestDataKey   = PerformanceTestingUtils.DATA_CLEANUP_KEY
  protected val cleanupTestData: Boolean = value(cleanupTestDataKey).forall(_.toBoolean)

  protected def setupInputData(timeout: Duration): (String, Long, Long) = {
    val client = ftpClient()
    try {
      if (!PerformanceTestingUtils.exists(client, csvOutputFolder))
        PerformanceTestingUtils.createFolder(client, csvOutputFolder)

      val result = generateData(
        numberOfRowsToFlush,
        timeout,
        (rows: Seq[Row]) => {
          val file        = s"$csvOutputFolder/${CommonUtils.randomString()}"
          val writer      = new BufferedWriter(new OutputStreamWriter(client.create(file)))
          val count       = new LongAdder()
          val sizeInBytes = new LongAdder()

          try {
            val cellNames: Set[String] = rows.head.cells().asScala.map(_.name).toSet
            writer
              .append(cellNames.mkString(","))
              .append("\n")
            rows.foreach(row => {
              val content = row.cells().asScala.map(_.value).mkString(",")
              count.increment()
              sizeInBytes.add(content.length)
              writer.append(content).append("\n")
            })
            (count.longValue(), sizeInBytes.longValue())
          } finally Releasable.close(writer)
        }
      )
      (csvOutputFolder, result._1, result._2)
    } finally Releasable.close(client)
  }

  protected[this] def ftpClient() =
    FileSystem.ftpBuilder
      .hostname(ftpHostname)
      .port(ftpPort)
      .user(ftpUser)
      .password(ftpPassword)
      .build
}
