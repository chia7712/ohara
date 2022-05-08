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

package oharastream.ohara.connector.jdbc

import oharastream.ohara.client.Enum

import scala.concurrent.duration.Duration

package object source {
  val DB_URL_KEY: String                = "source.db.url"
  val DB_USERNAME_KEY: String           = "source.db.username"
  val DB_PASSWORD_KEY: String           = "source.db.password"
  val DB_TABLENAME_KEY: String          = "source.table.name"
  val DB_CATALOG_PATTERN_KEY: String    = "source.schema.catalog"
  val DB_SCHEMA_PATTERN_KEY: String     = "source.schema.pattern"
  val TASK_TOTAL_KEY: String            = "task.total"
  val TASK_HASH_KEY: String             = "tash.hash"
  val FETCH_DATA_SIZE_KEY: String       = "source.jdbc.fetch.size"
  val FLUSH_DATA_SIZE_KEY: String       = "source.jdbc.flush.size"
  val TIMESTAMP_COLUMN_NAME_KEY: String = "source.timestamp.column.name"
  val INCREMENT_COLUMN_NAME_KEY: String = "source.increment.column.name"
  val FETCH_DATA_SIZE_DEFAULT: Int      = 1000
  val FLUSH_DATA_SIZE_DEFAULT: Int      = 1000
  val ORACLE_DB_NAME                    = "oracle"

  def toJavaDuration(d: Duration): java.time.Duration = java.time.Duration.ofMillis(d.toMillis)
  def toScalaDuration(d: java.time.Duration): Duration =
    Duration(d.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
}

abstract sealed class DatabaseProductName(val name: String)
object DatabaseProductName extends Enum[DatabaseProductName] {
  case object ORACLE extends DatabaseProductName("ORACLE")
}
