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

package oharastream.ohara.shabondi

import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.setting.WithDefinitions
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.shabondi.common.ShabondiUtils
import oharastream.ohara.shabondi.sink.WebServer

/**
  * the main class of shabondi sink. Don't remove this class as we need to get canonical class name.
  */
class ShabondiSink extends WithDefinitions with Releasable {
  private[this] var webServer: WebServer = _

  def start(args: Map[String, String]): Unit = {
    val config = new sink.SinkConfig(args)
    webServer = new sink.WebServer(config)
    webServer.start(CommonUtils.anyLocalAddress(), config.port)
  }

  override def close(): Unit = Releasable.close(webServer)
}

object ShabondiSink {
  private val log = Logger(ShabondiSink.getClass)

  def main(args: Array[String]): Unit = {
    val newArgs = ShabondiUtils.parseArgs(args)
    log.info("Arguments:")
    newArgs.foreach { case (k, v) => log.info(s"    $k=$v") }

    val sink = new ShabondiSink()
    try sink.start(newArgs)
    finally sink.close()
  }
}
