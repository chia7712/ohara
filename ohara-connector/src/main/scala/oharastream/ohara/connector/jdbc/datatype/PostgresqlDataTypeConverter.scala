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

class PostgresqlDataTypeConverter extends RDBDataTypeConverter {
  private[this] val TYPE_NAME_INT2        = "INT2"
  private[this] val TYPE_NAME_INT4        = "INT4"
  private[this] val TYPE_NAME_INT8        = "INT8"
  private[this] val TYPE_NAME_BIT         = "BIT"
  private[this] val TYPE_NAME_FLOAT4      = "FLOAT4"
  private[this] val TYPE_NAME_FLOAT8      = "FLOAT8"
  private[this] val TYPE_NAME_NUMERIC     = "NUMERIC"
  private[this] val TYPE_NAME_BPCHAR      = "BPCHAR"
  private[this] val TYPE_NAME_VARCHAR     = "VARCHAR"
  private[this] val TYPE_NAME_DATE        = "DATE"
  private[this] val TYPE_NAME_TIME        = "TIME"
  private[this] val TYPE_NAME_TIMETZ      = "TIMETZ"
  private[this] val TYPE_NAME_TIMESTAMP   = "TIMESTAMP"
  private[this] val TYPE_NAME_TIMESTAMPTZ = "TIMESTAMP"
  private[this] val TYPE_NAME_BYTEA       = "BYTEA"
  private[this] val TYPE_NAME_BOOL        = "BOOL"
  private[this] val TYPE_NAME_SERIAL      = "SERIAL"

  override protected[datatype] def dataBaseProductName: String = "postgresql"

  override protected[datatype] def converterDataType(column: RdbColumn): DataTypeEnum = {
    val typeName: String = column.dataType.toUpperCase
    typeName match {
      case TYPE_NAME_INT2 | TYPE_NAME_INT4 | TYPE_NAME_SERIAL =>
        DataTypeEnum.INTEGER
      case TYPE_NAME_INT8 =>
        DataTypeEnum.LONG
      case TYPE_NAME_BIT | TYPE_NAME_BOOL =>
        DataTypeEnum.BOOLEAN
      case TYPE_NAME_FLOAT4 =>
        DataTypeEnum.FLOAT
      case TYPE_NAME_FLOAT8 =>
        DataTypeEnum.DOUBLE
      case TYPE_NAME_NUMERIC =>
        DataTypeEnum.BIGDECIMAL
      case TYPE_NAME_BPCHAR | TYPE_NAME_VARCHAR =>
        DataTypeEnum.STRING
      case TYPE_NAME_DATE =>
        DataTypeEnum.DATE
      case TYPE_NAME_TIMETZ | TYPE_NAME_TIMESTAMP | TYPE_NAME_TIMESTAMPTZ =>
        DataTypeEnum.TIMESTAMP
      case TYPE_NAME_TIME =>
        DataTypeEnum.TIME
      case TYPE_NAME_BYTEA =>
        DataTypeEnum.BYTES
      case _ =>
        DataTypeEnum.NONETYPE
    }
  }
}
