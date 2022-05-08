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

@EnabledIfEnvironmentVariable(named = "ohara.it.performance.samba.hostname", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.samba.user", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.samba.password", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.samba.port", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.performance.samba.sharename", matches = ".*")
private[performance] abstract class BasicTestPerformance4Samba extends BasicTestPerformance {
  private[this] val SAMBA_HOSTNAME_KEY: String = "ohara.it.performance.samba.hostname"
  private[this] val SAMBA_USER_KEY: String     = "ohara.it.performance.samba.user"
  private[this] val SAMBA_PASSWORD_KEY: String = "ohara.it.performance.samba.password"
  private[this] val SAMBA_PORT_KEY: String     = "ohara.it.performance.samba.port"
  private[this] val SAMBA_SHARE_KEY: String    = "ohara.it.performance.samba.sharename"
  private[this] val sambaHostname: String      = sys.env(SAMBA_HOSTNAME_KEY)
  private[this] val sambaUsername: String      = sys.env(SAMBA_USER_KEY)
  private[this] val sambaPassword: String      = sys.env(SAMBA_PASSWORD_KEY)
  private[this] val sambaPort: Int             = sys.env(SAMBA_PORT_KEY).toInt
  private[this] val sambaShare: String         = sys.env(SAMBA_SHARE_KEY)

  private[this] val csvInputFolderKey       = PerformanceTestingUtils.CSV_INPUT_KEY
  private[this] val csvOutputFolder: String = value(csvInputFolderKey).getOrElse("input")

  private[this] val NEED_DELETE_DATA_KEY: String = PerformanceTestingUtils.DATA_CLEANUP_KEY
  protected[this] val needDeleteData: Boolean    = sys.env.getOrElse(NEED_DELETE_DATA_KEY, "true").toBoolean

  protected val sambaSettings: Map[String, JsValue] = Map(
    oharastream.ohara.connector.smb.SMB_HOSTNAME_KEY   -> JsString(sambaHostname),
    oharastream.ohara.connector.smb.SMB_PORT_KEY       -> JsNumber(sambaPort),
    oharastream.ohara.connector.smb.SMB_USER_KEY       -> JsString(sambaUsername),
    oharastream.ohara.connector.smb.SMB_PASSWORD_KEY   -> JsString(sambaPassword),
    oharastream.ohara.connector.smb.SMB_SHARE_NAME_KEY -> JsString(sambaShare)
  )

  protected def setupInputData(timeout: Duration): (String, Long, Long) = {
    val client = sambaClient()
    try {
      if (!client.exists(csvOutputFolder)) PerformanceTestingUtils.createFolder(client, csvOutputFolder)

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
              writer
                .append(content)
                .append("\n")
            })
            (count.longValue(), sizeInBytes.longValue())
          } finally Releasable.close(writer)
        }
      )
      (csvOutputFolder, result._1, result._2)
    } finally Releasable.close(client)
  }

  protected[this] def sambaClient(): FileSystem =
    FileSystem.smbBuilder
      .hostname(sambaHostname)
      .port(sambaPort)
      .user(sambaUsername)
      .password(sambaPassword)
      .shareName(sambaShare)
      .build()
}
