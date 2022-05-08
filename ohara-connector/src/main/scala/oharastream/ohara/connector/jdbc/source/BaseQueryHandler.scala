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

import java.sql.Timestamp

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Cell, Column, DataType, Row}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.jdbc.DatabaseProductName.ORACLE
import oharastream.ohara.connector.jdbc.util.ColumnInfo
import oharastream.ohara.kafka.connector.RowSourceRecord

trait BaseQueryHandler extends Releasable {
  protected[this] val client: DatabaseClient

  /**
    * Get database product name
    * @return product name
    */
  protected[this] def dbProduct: String = client.connection.getMetaData.getDatabaseProductName

  /**
    * Query table data from the database
    * @param key split task
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return
    */
  protected[source] def queryData(
    key: String,
    startTimestamp: Timestamp,
    stopTimestamp: Timestamp
  ): Seq[RowSourceRecord]

  /**
    * Confirm data write to the topic
    * The start timestamp and stop timestamp range can't change.
    * @param key split task
    * @param startTimestamp start timestamp
    * @param stopTimestamp stop timestamp
    * @return true or false
    */
  protected[source] def completed(key: String, startTimestamp: Timestamp, stopTimestamp: Timestamp): Boolean

  /**
    * Query first row from the database table
    * @param config JDBCSourceConnector setting
    * @return timestamp
    */
  protected[source] def tableFirstTimestampValue(config: JDBCSourceConnectorConfig): Timestamp = {
    val sql = dbProduct.toUpperCase match {
      case ORACLE.name =>
        s"SELECT ${config.timestampColumnName} FROM ${config.dbTableName} ORDER BY ${config.timestampColumnName} FETCH FIRST 1 ROWS ONLY"
      case _ =>
        s"SELECT ${config.timestampColumnName} FROM ${config.dbTableName} ORDER BY ${config.timestampColumnName} LIMIT 1"
    }

    val preparedStatement = client.connection.prepareStatement(sql)
    try {
      val resultSet = preparedStatement.executeQuery()
      try {
        if (resultSet.next()) resultSet.getTimestamp(config.timestampColumnName)
        else new Timestamp(CommonUtils.current())
      } finally Releasable.close(resultSet)
    } finally Releasable.close(preparedStatement)
  }

  /**
    * Query current timestamp from the database
    * @return timestamp
    */
  protected[source] def current(): Timestamp = {
    val query = dbProduct.toUpperCase match {
      case ORACLE.name => "SELECT CURRENT_TIMESTAMP FROM dual"
      case _           => "SELECT CURRENT_TIMESTAMP;"
    }
    val stmt = client.connection.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      try {
        if (rs.next()) rs.getTimestamp(1) else new Timestamp(0)
      } finally Releasable.close(rs)
    } finally Releasable.close(stmt)
  }

  override def close(): Unit = Releasable.close(client)

  private[source] def columns(client: DatabaseClient, tableName: String): Seq[RdbColumn] =
    client.tableQuery.tableName(tableName).execute().head.columns

  private[source] def row(schema: Seq[Column], columns: Seq[ColumnInfo[_]]): Row =
    Row.of(
      schema
        .sortBy(_.order)
        .map(s => (s, values(s.name, columns)))
        .map {
          case (s, value) =>
            Cell.of(
              s.newName,
              convertToValue(s, value)
            )
        }: _*
    )

  private[source] def convertToValue(column: Column, value: Any): Any = {
    // Confirm all data type is java.lang.*
    column.dataType match {
      case DataType.BOOLEAN => java.lang.Boolean.valueOf(value.asInstanceOf[Boolean])
      case DataType.SHORT   => java.lang.Short.valueOf(value.asInstanceOf[Short])
      case DataType.INT     => java.lang.Integer.valueOf(value.asInstanceOf[Int])
      case DataType.LONG    => java.lang.Long.valueOf(value.asInstanceOf[Long])
      case DataType.FLOAT   => java.lang.Float.valueOf(value.asInstanceOf[Float])
      case DataType.DOUBLE  => java.lang.Double.valueOf(value.asInstanceOf[Double])
      case DataType.BYTE    => java.lang.Byte.valueOf(value.asInstanceOf[Byte])
      case DataType.BYTES   => value.asInstanceOf[Array[java.lang.Byte]]
      case DataType.STRING  => java.lang.String.valueOf(value.asInstanceOf[String])
      case DataType.OBJECT  => value
      case _ =>
        throw new IllegalArgumentException(s"${column.newName()} column unsupported the ${column.dataType} type...")
    }
  }

  private[this] def values(schemaColumnName: String, dbColumnInfo: Seq[ColumnInfo[_]]): Any =
    dbColumnInfo
      .find(_.columnName == schemaColumnName)
      .map(_.value)
      .getOrElse(throw new RuntimeException(s"Database table not have the $schemaColumnName column"))
}
