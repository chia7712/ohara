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

package oharastream.ohara.it.connector.jdbc

import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.connector.jdbc.source.JDBCSourceConnectorConfig
import oharastream.ohara.kafka.connector.TaskSetting
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import scala.jdk.CollectionConverters._

@EnabledIfEnvironmentVariable(named = "ohara.it.postgresql.db.url", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.postgresql.db.username", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.postgresql.db.password", matches = ".*")
class TestPostgresqlTimestampJDBCSourceConnector extends BasicTestConnectorCollie {
  override protected[jdbc] def dbUrl: String      = sys.env("ohara.it.postgresql.db.url")
  override protected[jdbc] def dbUserName: String = sys.env("ohara.it.postgresql.db.username")
  override protected[jdbc] def dbPassword: String = sys.env("ohara.it.postgresql.db.password")
  override protected[jdbc] def dbName: String     = "postgresql"

  override protected[jdbc] val tableName: String = s"table${CommonUtils.randomString(5)}"

  override protected[jdbc] val jdbcDriverJarFileName: String = "postgresql-42.2.6.jar"

  override protected[jdbc] val columnPrefixName: String = "column"

  override protected[jdbc] val BINARY_TYPE_NAME: String = "BYTEA"

  override protected[jdbc] val INCREMENT_TYPE_NAME: String = "SERIAL"

  override protected[jdbc] val props: JDBCSourceConnectorConfig = JDBCSourceConnectorConfig(
    TaskSetting.of(
      Map(
        "source.db.url"                -> dbUrl,
        "source.db.username"           -> dbUserName,
        "source.db.password"           -> dbPassword,
        "source.table.name"            -> tableName,
        "source.timestamp.column.name" -> timestampColumn
      ).asJava
    )
  )
}
