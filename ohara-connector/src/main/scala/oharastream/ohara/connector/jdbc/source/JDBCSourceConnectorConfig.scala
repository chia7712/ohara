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

package oharastream.ohara.connector.jdbc.source

import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.TaskSetting

/**
  * This class is getting property value
  */
case class JDBCSourceConnectorConfig(
  dbURL: String,
  dbUserName: String,
  dbPassword: String,
  dbTableName: String,
  dbCatalogPattern: Option[String],
  dbSchemaPattern: Option[String],
  fetchDataSize: Int,
  flushDataSize: Int,
  timestampColumnName: String,
  incrementColumnName: Option[String],
  taskTotal: Int,
  taskHash: Int
) {
  def toMap: Map[String, String] =
    Map(
      DB_URL_KEY                -> dbURL,
      DB_USERNAME_KEY           -> dbUserName,
      DB_PASSWORD_KEY           -> dbPassword,
      DB_TABLENAME_KEY          -> dbTableName,
      FETCH_DATA_SIZE_KEY       -> fetchDataSize.toString,
      FLUSH_DATA_SIZE_KEY       -> flushDataSize.toString,
      TIMESTAMP_COLUMN_NAME_KEY -> timestampColumnName,
      TASK_TOTAL_KEY            -> taskTotal.toString,
      TASK_HASH_KEY             -> taskHash.toString
    ) ++ dbCatalogPattern.map(s => Map(DB_CATALOG_PATTERN_KEY    -> s)).getOrElse(Map.empty) ++
      dbSchemaPattern.map(s => Map(DB_SCHEMA_PATTERN_KEY         -> s)).getOrElse(Map.empty) ++
      incrementColumnName.map(s => Map(INCREMENT_COLUMN_NAME_KEY -> s)).getOrElse(Map.empty)
}

object JDBCSourceConnectorConfig {
  def apply(settings: TaskSetting): JDBCSourceConnectorConfig = {
    JDBCSourceConnectorConfig(
      dbURL = settings.stringValue(DB_URL_KEY),
      dbUserName = settings.stringValue(DB_USERNAME_KEY),
      dbPassword = settings.stringValue(DB_PASSWORD_KEY),
      dbTableName = settings.stringValue(DB_TABLENAME_KEY),
      dbCatalogPattern =
        Option(settings.stringOption(DB_CATALOG_PATTERN_KEY).orElse(null)).filterNot(CommonUtils.isEmpty),
      dbSchemaPattern = Option(settings.stringOption(DB_SCHEMA_PATTERN_KEY).orElse(null)).filterNot(CommonUtils.isEmpty),
      fetchDataSize = settings.intOption(FETCH_DATA_SIZE_KEY).orElse(FETCH_DATA_SIZE_DEFAULT),
      flushDataSize = settings.intOption(FLUSH_DATA_SIZE_KEY).orElse(FLUSH_DATA_SIZE_DEFAULT),
      timestampColumnName = settings.stringValue(TIMESTAMP_COLUMN_NAME_KEY),
      incrementColumnName =
        Option(settings.stringOption(INCREMENT_COLUMN_NAME_KEY).orElse(null)).filterNot(CommonUtils.isEmpty),
      taskTotal = settings.intOption(TASK_TOTAL_KEY).orElse(0),
      taskHash = settings.intOption(TASK_HASH_KEY).orElse(0)
    )
  }
}
