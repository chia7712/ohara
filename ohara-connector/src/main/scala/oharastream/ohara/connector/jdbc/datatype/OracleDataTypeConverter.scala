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

package oharastream.ohara.connector.jdbc.datatype
import oharastream.ohara.client.configurator.InspectApi.RdbColumn

class OracleDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_CHAR: String                   = "CHAR"
  private[this] val TYPE_NAME_CHARACTER: String              = "CHARACTER"
  private[this] val TYPE_NAME_LONG: String                   = "LONG"
  private[this] val TYPE_NAME_VARCHAR: String                = "VARCHAR"
  private[this] val TYPE_NAME_VARCHAR2: String               = "VARCHAR2"
  private[this] val TYPE_NAME_NCHAR: String                  = "NCHAR"
  private[this] val TYPE_NAME_NVARCHAR2: String              = "NVARCHAR2"
  private[this] val TYPE_NAME_RAW: String                    = "RAW"
  private[this] val TYPE_NAME_LONGRAW: String                = "LONG RAW"
  private[this] val TYPE_NAME_INT: String                    = "INT"
  private[this] val TYPE_NAME_INTEGER: String                = "INTEGER"
  private[this] val TYPE_NAME_DEC: String                    = "DEC"
  private[this] val TYPE_NAME_DECIMAL: String                = "DECIMAL"
  private[this] val TYPE_NAME_NUMBER: String                 = "NUMBER"
  private[this] val TYPE_NAME_NUMERIC: String                = "NUMERIC"
  private[this] val TYPE_NAME_DOUBLE_PRECISION: String       = "DOUBLE PRECISION"
  private[this] val TYPE_NAME_FLOAT: String                  = "FLOAT"
  private[this] val TYPE_NAME_SMALLINT: String               = "SMALLINT"
  private[this] val TYPE_NAME_REAL: String                   = "REAL"
  private[this] val TYPE_NAME_DATE: String                   = "DATE"
  private[this] val TYPE_NAME_TIMESTAMP: String              = "TIMESTAMP"
  private[this] val TYPE_NAME_INTERVAL_YEAR_TO_MONTH: String = "INTERVAL YEAR TO MONTH"
  private[this] val TYPE_NAME_INTERVAL_DAY_TO_SECOND: String = "INTERVAL DAY TO SECOND"

  override protected[datatype] def dataBaseProductName: String = "oracle"

  override protected[datatype] def converterDataType(column: RdbColumn): DataTypeEnum = {
    val typeName: String = column.dataType.toUpperCase
    if (typeName.startsWith(TYPE_NAME_INT) || typeName.startsWith(TYPE_NAME_INTEGER) || typeName.startsWith(
          TYPE_NAME_SMALLINT
        ) ||
        typeName.startsWith(TYPE_NAME_DEC) || typeName.startsWith(TYPE_NAME_DECIMAL) || typeName.startsWith(
          TYPE_NAME_NUMBER
        ) ||
        typeName.startsWith(TYPE_NAME_NUMERIC))
      DataTypeEnum.INTEGER
    else if (typeName.startsWith(TYPE_NAME_REAL))
      DataTypeEnum.FLOAT
    else if (typeName.startsWith(TYPE_NAME_DOUBLE_PRECISION) || typeName.startsWith(TYPE_NAME_FLOAT))
      DataTypeEnum.DOUBLE
    else if (typeName.startsWith(TYPE_NAME_CHAR) || typeName.startsWith(TYPE_NAME_CHARACTER) || typeName.startsWith(
               TYPE_NAME_LONG
             ) || typeName.startsWith(TYPE_NAME_VARCHAR) || typeName.startsWith(TYPE_NAME_VARCHAR2) ||
             typeName.startsWith(TYPE_NAME_NCHAR) || typeName.startsWith(TYPE_NAME_NVARCHAR2) || typeName.startsWith(
               TYPE_NAME_INTERVAL_YEAR_TO_MONTH
             ) || typeName.startsWith(TYPE_NAME_INTERVAL_DAY_TO_SECOND))
      DataTypeEnum.STRING
    else if (typeName.startsWith(TYPE_NAME_DATE))
      DataTypeEnum.DATE
    else if (typeName.startsWith(TYPE_NAME_TIMESTAMP))
      DataTypeEnum.TIMESTAMP
    else if (typeName.startsWith(TYPE_NAME_RAW) || typeName == TYPE_NAME_LONGRAW)
      DataTypeEnum.BYTES
    else
      DataTypeEnum.NONETYPE
  }
}
