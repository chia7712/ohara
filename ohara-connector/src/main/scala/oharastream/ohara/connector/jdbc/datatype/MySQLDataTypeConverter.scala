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

class MySQLDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_BIT                = "BIT"
  private[this] val TYPE_NAME_TINYINT: String    = "TINYINT"
  private[this] val TYPE_NAME_BOOL: String       = "BOOL"
  private[this] val TYPE_NAME_BOOLEAN: String    = "BOOLEAN"
  private[this] val TYPE_NAME_SMALLINT: String   = "SMALLINT"
  private[this] val TYPE_NAME_MEDIUMINT: String  = "MEDIUMINT"
  private[this] val TYPE_NAME_INT: String        = "INT"
  private[this] val TYPE_NAME_INTEGER: String    = "INTEGER"
  private[this] val TYPE_NAME_BIGINT: String     = "BIGINT"
  private[this] val TYPE_NAME_FLOAT: String      = "FLOAT"
  private[this] val TYPE_NAME_DOUBLE: String     = "DOUBLE"
  private[this] val TYPE_NAME_DECIMAL: String    = "DECIMAL"
  private[this] val TYPE_NAME_DATE: String       = "DATE"
  private[this] val TYPE_NAME_DATETIME: String   = "DATETIME"
  private[this] val TYPE_NAME_TIMESTAMP: String  = "TIMESTAMP"
  private[this] val TYPE_NAME_TIME: String       = "TIME"
  private[this] val TYPE_NAME_CHAR: String       = "CHAR"
  private[this] val TYPE_NAME_VARCHAR: String    = "VARCHAR"
  private[this] val TYPE_NAME_BINARY: String     = "BINARY"
  private[this] val TYPE_NAME_VARBINARY: String  = "VARBINARY"
  private[this] val TYPE_NAME_TINYBLOB: String   = "TINYBLOB"
  private[this] val TYPE_NAME_BLOB: String       = "BLOB"
  private[this] val TYPE_NAME_TINYTEXT: String   = "TINYTEXT"
  private[this] val TYPE_NAME_TEXT: String       = "TEXT"
  private[this] val TYPE_NAME_MEDIUMBLOB: String = "MEDIUMBLOB"
  private[this] val TYPE_NAME_MEDIUMTEXT: String = "MEDIUMTEXT"
  private[this] val TYPE_NAME_LONGBLOB: String   = "LONGBLOB"
  private[this] val TYPE_NAME_LONGTEXT: String   = "LONGTEXT"
  private[this] val TYPE_NAME_ENUM: String       = "ENUM"
  private[this] val TYPE_NAME_SET: String        = "SET"

  override protected[datatype] def dataBaseProductName: String = "mysql"

  override protected[datatype] def converterDataType(column: RdbColumn): DataTypeEnum = {
    val typeName: String = column.dataType.toUpperCase
    typeName match {
      case TYPE_NAME_TINYINT | TYPE_NAME_SMALLINT | TYPE_NAME_MEDIUMINT | TYPE_NAME_INT | TYPE_NAME_INTEGER =>
        DataTypeEnum.INTEGER
      case TYPE_NAME_BIGINT =>
        DataTypeEnum.LONG
      case TYPE_NAME_BIT | TYPE_NAME_BOOL | TYPE_NAME_BOOLEAN =>
        DataTypeEnum.BOOLEAN
      case TYPE_NAME_FLOAT =>
        DataTypeEnum.FLOAT
      case TYPE_NAME_DOUBLE =>
        DataTypeEnum.DOUBLE
      case TYPE_NAME_DECIMAL =>
        DataTypeEnum.BIGDECIMAL
      case TYPE_NAME_CHAR | TYPE_NAME_VARCHAR | TYPE_NAME_TINYTEXT | TYPE_NAME_TEXT | TYPE_NAME_MEDIUMTEXT |
          TYPE_NAME_LONGTEXT | TYPE_NAME_ENUM | TYPE_NAME_SET =>
        DataTypeEnum.STRING
      case TYPE_NAME_DATE =>
        DataTypeEnum.DATE
      case TYPE_NAME_DATETIME | TYPE_NAME_TIMESTAMP =>
        DataTypeEnum.TIMESTAMP
      case TYPE_NAME_BINARY | TYPE_NAME_VARBINARY | TYPE_NAME_TINYBLOB | TYPE_NAME_BLOB | TYPE_NAME_MEDIUMBLOB |
          TYPE_NAME_LONGBLOB =>
        DataTypeEnum.BYTES
      case TYPE_NAME_TIME =>
        DataTypeEnum.TIME
      case _ =>
        DataTypeEnum.NONETYPE
    }
  }
}
