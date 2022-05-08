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

import java.sql.{Date, ResultSet, Time, Timestamp}
import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.connector.jdbc.util.DateTimeUtils

trait RDBDataTypeConverter {
  /**
    * Converter result data type to Java object
    * @param resultSet JDBC query the ResultSet
    * @param column column info
    * @return data type object
    */
  def converterValue(resultSet: ResultSet, column: RdbColumn): Any = {
    val columnName             = column.name
    val typeName               = column.dataType.toUpperCase
    val dataType: DataTypeEnum = converterDataType(column)
    dataType match {
      case DataTypeEnum.INTEGER =>
        java.lang.Integer.valueOf(resultSet.getInt(columnName))
      case DataTypeEnum.LONG =>
        java.lang.Long.valueOf(resultSet.getLong(columnName))
      case DataTypeEnum.BOOLEAN =>
        java.lang.Boolean.valueOf(resultSet.getBoolean(columnName))
      case DataTypeEnum.FLOAT =>
        java.lang.Float.valueOf(resultSet.getFloat(columnName))
      case DataTypeEnum.DOUBLE =>
        java.lang.Double.valueOf(resultSet.getDouble(columnName))
      case DataTypeEnum.BIGDECIMAL =>
        Option(resultSet.getBigDecimal(columnName)).getOrElse(new java.math.BigDecimal(0L))
      case DataTypeEnum.STRING =>
        Option(resultSet.getString(columnName)).getOrElse("null")
      case DataTypeEnum.DATE =>
        Option(resultSet.getDate(columnName, DateTimeUtils.CALENDAR)).getOrElse(new Date(0))
      case DataTypeEnum.TIME =>
        Option(resultSet.getTime(columnName, DateTimeUtils.CALENDAR)).getOrElse(new Time(0))
      case DataTypeEnum.TIMESTAMP =>
        Option(resultSet.getTimestamp(columnName, DateTimeUtils.CALENDAR)).getOrElse(new Timestamp(0))
      case DataTypeEnum.BYTES =>
        Option(resultSet.getBytes(columnName))
          .map(value => value.map(x => java.lang.Byte.valueOf(x)))
          .getOrElse(Array.empty)
          .asInstanceOf[Array[java.lang.Byte]]
      case _ =>
        throw new UnsupportedOperationException(
          s"JDBC Source Connector not support $typeName data type in $columnName column for $dataBaseProductName implement."
        )
    }
  }
  protected[datatype] def dataBaseProductName: String

  protected[datatype] def converterDataType(column: RdbColumn): DataTypeEnum
}
