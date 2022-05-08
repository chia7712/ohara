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

class GenericDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_BOOLEAN: String     = "BOOLEAN"
  private[this] val TYPE_NAME_BIT: String         = "BIT"
  private[this] val TYPE_NAME_INTEGER: String     = "INT"
  private[this] val TYPE_NAME_BIGINT: String      = "BIGINT"
  private[this] val TYPE_NAME_FLOAT: String       = "FLOAT"
  private[this] val TYPE_NAME_DOUBLE: String      = "DOUBLE"
  private[this] val TYPE_NAME_CHAR: String        = "CHAR"
  private[this] val TYPE_NAME_VARCHAR: String     = "VARCHAR"
  private[this] val TYPE_NAME_LONGVARCHAR: String = "LONGVARCHAR"
  private[this] val TYPE_NAME_TIMESTAMP: String   = "TIMESTAMP"
  private[this] val TYPE_NAME_DATE: String        = "DATE"
  private[this] val TYPE_NAME_TIME: String        = "TIME"
  private[this] val TYPE_NAME_VARCHAR2: String    = "VARCHAR2"
  private[this] val TYPE_NAME_NUMBER: String      = "NUMBER"

  override protected[datatype] def dataBaseProductName: String = "generic"

  override protected[datatype] def converterDataType(column: RdbColumn): DataTypeEnum = {
    val typeName: String = column.dataType.toUpperCase
    typeName match {
      case TYPE_NAME_INTEGER | TYPE_NAME_NUMBER =>
        DataTypeEnum.INTEGER
      case TYPE_NAME_BIGINT =>
        DataTypeEnum.LONG
      case TYPE_NAME_BOOLEAN =>
        DataTypeEnum.BOOLEAN
      case TYPE_NAME_FLOAT =>
        DataTypeEnum.FLOAT
      case TYPE_NAME_DOUBLE =>
        DataTypeEnum.DOUBLE
      case TYPE_NAME_CHAR | TYPE_NAME_VARCHAR | TYPE_NAME_LONGVARCHAR | TYPE_NAME_VARCHAR2 =>
        DataTypeEnum.STRING
      case TYPE_NAME_DATE =>
        DataTypeEnum.DATE
      case TYPE_NAME_TIME =>
        DataTypeEnum.TIME
      case TYPE_NAME_TIMESTAMP =>
        DataTypeEnum.TIMESTAMP
      case TYPE_NAME_BIT =>
        DataTypeEnum.BYTES
      case _ =>
        DataTypeEnum.NONETYPE
    }
  }
}
