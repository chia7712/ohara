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
import java.util.Objects

import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.{Column, DataType}
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.Releasable
import oharastream.ohara.connector.jdbc.datatype.RDBDataTypeConverterFactory
import oharastream.ohara.connector.jdbc.util.DateTimeUtils
import oharastream.ohara.kafka.connector.{RowSourceContext, RowSourceRecord}

trait TimestampQueryHandler extends BaseQueryHandler {
  def config: JDBCSourceConnectorConfig
  def rowSourceContext: RowSourceContext
  def topics: Seq[TopicKey]
  def schema: Seq[Column]
  def offsetCache: JDBCOffsetCache
}

object TimestampQueryHandler {
  def builder: Builder = new Builder()

  class Builder private[source] extends oharastream.ohara.common.pattern.Builder[TimestampQueryHandler] {
    private[this] var config: JDBCSourceConnectorConfig  = _
    private[this] var rowSourceContext: RowSourceContext = _
    private[this] var topics: Seq[TopicKey]              = _
    private[this] var schema: Seq[Column]                = _

    def config(config: JDBCSourceConnectorConfig): Builder = {
      this.config = Objects.requireNonNull(config)
      this
    }

    def rowSourceContext(rowSourceContext: RowSourceContext): Builder = {
      this.rowSourceContext = Objects.requireNonNull(rowSourceContext)
      this
    }

    def topics(topics: Seq[TopicKey]): Builder = {
      this.topics = topics
      this
    }

    def schema(schema: Seq[Column]): Builder = {
      this.schema = schema
      this
    }

    override def build(): TimestampQueryHandler = new TimestampQueryHandler {
      override val offsetCache: JDBCOffsetCache       = new JDBCOffsetCache()
      override val config: JDBCSourceConnectorConfig  = Builder.this.config
      override val rowSourceContext: RowSourceContext = Builder.this.rowSourceContext
      override val topics: Seq[TopicKey]              = Builder.this.topics
      override val schema: Seq[Column]                = Builder.this.schema
      override protected[this] val client: DatabaseClient = DatabaseClient.builder
        .url(config.dbURL)
        .user(config.dbUserName)
        .password(config.dbPassword)
        .build
      client.connection.setAutoCommit(false)
      override protected[source] def queryData(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Seq[RowSourceRecord] = {
        val tableName           = config.dbTableName
        val timestampColumnName = config.timestampColumnName
        offsetCache.loadIfNeed(rowSourceContext, key)

        val sql =
          s"SELECT * FROM $tableName WHERE $timestampColumnName >= ? AND $timestampColumnName < ? ORDER BY $timestampColumnName"
        val prepareStatement = client.connection.prepareStatement(sql)
        try {
          prepareStatement.setFetchSize(config.fetchDataSize)
          prepareStatement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          prepareStatement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = prepareStatement.executeQuery()
          try {
            val rdbDataTypeConverter = RDBDataTypeConverterFactory.dataTypeConverter(dbProduct)
            val rdbColumnInfo        = columns(client, tableName)
            val results              = new QueryResultIterator(rdbDataTypeConverter, resultSet, rdbColumnInfo)
            val offset               = offsetCache.readOffset(key)
            results.zipWithIndex
              .filter {
                case (_, index) =>
                  index >= offset
              }
              .take(config.flushDataSize)
              .flatMap {
                case (columns, rowIndex) =>
                  val newSchema =
                    if (schema.isEmpty)
                      columns.map(c => Column.builder().name(c.columnName).dataType(DataType.OBJECT).order(0).build())
                    else schema
                  val offset = rowIndex + 1
                  offsetCache.update(key, offset)
                  topics.map(
                    RowSourceRecord
                      .builder()
                      .sourcePartition(java.util.Map.of(JDBCOffsetCache.TABLE_PARTITION_KEY, key))
                      //Writer Offset
                      .sourceOffset(
                        java.util.Map.of(JDBCOffsetCache.TABLE_OFFSET_KEY, offset.toString)
                      )
                      //Create Ohara Row
                      .row(row(newSchema, columns))
                      .topicKey(_)
                      .build()
                  )
              }
              .toSeq
          } finally Releasable.close(resultSet)
        } finally {
          Releasable.close(prepareStatement)
          // Use the JDBC fetchSize function, should setting setAutoCommit function to false.
          // Confirm this connection ResultSet to update, need to call connection commit function.
          // Release any database locks currently held by this Connection object
          client.connection.commit()
        }
      }

      override protected[source] def completed(
        key: String,
        startTimestamp: Timestamp,
        stopTimestamp: Timestamp
      ): Boolean = {
        val dbCount     = count(startTimestamp, stopTimestamp)
        val offsetIndex = offsetCache.readOffset(key)
        if (dbCount < offsetIndex)
          throw new IllegalArgumentException(
            s"The $startTimestamp~$stopTimestamp data offset value over the table count. Please confirm your data"
          )
        else offsetIndex == dbCount
      }

      private[this] def count(startTimestamp: Timestamp, stopTimestamp: Timestamp) = {
        val sql =
          s"SELECT COUNT(*) FROM ${config.dbTableName} WHERE ${config.timestampColumnName} >= ? AND ${config.timestampColumnName} < ?"

        val statement = client.connection.prepareStatement(sql)
        try {
          statement.setTimestamp(1, startTimestamp, DateTimeUtils.CALENDAR)
          statement.setTimestamp(2, stopTimestamp, DateTimeUtils.CALENDAR)
          val resultSet = statement.executeQuery()
          try if (resultSet.next()) resultSet.getInt(1)
          else 0
          finally Releasable.close(resultSet)
        } finally {
          Releasable.close(statement)
          // Use the JDBC fetchSize function, should setting setAutoCommit function to false.
          // Confirm this connection ResultSet to update, need to call connection commit function.
          // Release any database locks currently held by this Connection object
          client.connection.commit()
        }
      }
    }
  }
}
