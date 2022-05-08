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

@EnabledIfEnvironmentVariable(named = "ohara.it.oracle.db.url", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.oracle.db.username", matches = ".*")
@EnabledIfEnvironmentVariable(named = "ohara.it.oracle.db.password", matches = ".*")
class TestOracleTimestampJDBCSourceConnector extends BasicTestConnectorCollie {
  override protected[jdbc] val dbUrl: String      = sys.env("ohara.it.oracle.db.url")
  override protected[jdbc] val dbUserName: String = sys.env("ohara.it.oracle.db.username")
  override protected[jdbc] val dbPassword: String = sys.env("ohara.it.oracle.db.password")

  override protected[jdbc] val dbName: String = "oracle"

  override protected[jdbc] val tableName: String = s"TABLE${CommonUtils.randomString(5)}".toUpperCase

  override protected[jdbc] val jdbcDriverJarFileName: String = "ojdbc8.jar"

  override protected[jdbc] val columnPrefixName: String = "COLUMN"

  override protected[jdbc] val BINARY_TYPE_NAME: String = "RAW(30)"

  override protected[jdbc] val INCREMENT_TYPE_NAME: String = "NUMBER GENERATED ALWAYS AS IDENTITY"

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
