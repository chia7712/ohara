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

package oharastream.ohara.shabondi.common

import com.typesafe.scalalogging.Logger
import oharastream.ohara.common.util.{CommonUtils, VersionUtils}

import scala.jdk.CollectionConverters._
object ShabondiUtils {
  private val log = Logger(ShabondiUtils.getClass)

  private val ESCAPE_STRING1 = "_____"
  private val ESCAPE_STRING2 = "~~~~~"

  def escape(value: String): String = {
    if (value.contains(ESCAPE_STRING1))
      throw new IllegalArgumentException(s"Cannot escape the value `$value` by escape string $ESCAPE_STRING1")
    if (value.contains(ESCAPE_STRING2))
      throw new IllegalArgumentException(s"Cannot escape the value `$value` by escape string $ESCAPE_STRING2")
    value
      .replaceAll("\"", ESCAPE_STRING1)
      .replaceAll(" ", ESCAPE_STRING2)
  }

  def unescape(value: String): String = {
    value
      .replaceAll(ESCAPE_STRING1, "\"")
      .replaceAll(ESCAPE_STRING2, " ")
  }

  def parseArgs(args: Array[String]): Map[String, String] =
    CommonUtils
      .parse(args.toSeq.asJava)
      .asScala
      .toMap
      .map {
        case (k, v) =>
          (k, unescape(v))
      }
  def logArgs(args: Map[String, String]): Unit = {
    log.info("Arguments:")
    args.foreach { case (k, v) => log.info(s"    $k=$v") }
  }

  def apiUrl: String = s"https://oharastream.github.io/en/docs/${VersionUtils.BRANCH}/shabondi/"
}
