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

package oharastream.ohara.it.stream
import java.util

import oharastream.ohara.common.data.Row
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.stream.config.StreamSetting
import oharastream.ohara.stream.{OStream, Stream}
import scala.jdk.CollectionConverters._

/**
  * This is a sample stream that will do nothing but write data to output topic
  * It is not placed at test scope since we need jar when tests manually.
  */
class DumbStream extends Stream {
  private val FILTER_HEADER_KEY = "FILTER_HEADER_KEY"
  private val FILTER_VALUE_KEY  = "FILTER_VALUE_KEY"

  override protected def customSettingDefinitions(): util.Map[String, SettingDef] =
    Map(
      FILTER_HEADER_KEY -> SettingDef
        .builder()
        .key(FILTER_HEADER_KEY)
        .displayName("header name to be filtered")
        .optional(SettingDef.Type.STRING)
        .build(),
      FILTER_VALUE_KEY -> SettingDef
        .builder()
        .key(FILTER_VALUE_KEY)
        .displayName("column value to be filtered")
        .optional(SettingDef.Type.STRING)
        .build()
    ).asJava

  override def start(ostream: OStream[Row], configs: StreamSetting): Unit = {
    ostream
      .filter(row => {
        val headerName = configs.string(FILTER_HEADER_KEY)
        val value      = configs.string(FILTER_VALUE_KEY)

        if (headerName.isPresent) {
          if (value.isPresent) {
            row.names().contains(headerName.get()) && row
              .cell(headerName.get())
              .value()
              .toString
              .equalsIgnoreCase(value.get())
          } else {
            // if no value defined, we return rows which contain the header
            row.names().contains(headerName.get())
          }
        } else
          // if no header and value defined, we return all rows
          true
      })
      .start()
  }
}
