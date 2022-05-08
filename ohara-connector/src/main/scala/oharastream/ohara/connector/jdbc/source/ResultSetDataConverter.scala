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

import java.sql.ResultSet

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.connector.jdbc.datatype.RDBDataTypeConverter
import oharastream.ohara.connector.jdbc.util.ColumnInfo

/**
  * This class for converter the ResultSet data
  */
object ResultSetDataConverter {
  /**
    * Converter the ResultSet a record data
    * @param resultSet
    * @param columns
    * @return
    */
  protected[source] def converterRecord(
    rdbDataTypeConverter: RDBDataTypeConverter,
    resultSet: ResultSet,
    columns: Seq[RdbColumn]
  ): Seq[ColumnInfo[_]] = {
    columns.map(column => {
      val resultValue: Any = rdbDataTypeConverter.converterValue(resultSet, column)
      // Setting data value to ColumnInfo case class
      ColumnInfo(column.name, column.dataType, resultValue)
    })
  }
}
