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

import oharastream.ohara.common.util.{Releasable, Sleeper}
import oharastream.ohara.kafka.connector._

import scala.jdk.CollectionConverters._

class JDBCSourceTask extends RowSourceTask {
  private[this] val TIMESTAMP_PARTITION_RANGE: Int    = 86400000 // 1 day
  private[this] var firstTimestampValue: Timestamp    = _
  private[this] var config: JDBCSourceConnectorConfig = _
  private[this] var queryHandler: BaseQueryHandler    = _

  override protected[source] def run(settings: TaskSetting): Unit = {
    config = JDBCSourceConnectorConfig(settings)
    queryHandler = config.incrementColumnName
      .map { incrementColumnName =>
        TimestampIncrementQueryHandler.builder
          .config(config)
          .incrementColumnName(incrementColumnName)
          .rowSourceContext(rowContext)
          .topics(settings.topicKeys().asScala.toSeq)
          .schema(settings.columns.asScala.toSeq)
          .build()
      }
      .getOrElse {
        TimestampQueryHandler.builder
          .config(config)
          .rowSourceContext(rowContext)
          .topics(settings.topicKeys().asScala.toSeq)
          .schema(settings.columns.asScala.toSeq)
          .build()
      }
    firstTimestampValue = queryHandler.tableFirstTimestampValue(config)
  }

  override protected[source] def pollRecords(): java.util.List[RowSourceRecord] = {
    val sleeper = new Sleeper()
    do {
      val timestampRange = calcTimestampRange(firstTimestampValue, firstTimestampValue)
      var startTimestamp = timestampRange._1
      var stopTimestamp  = replaceToCurrentTimestamp(timestampRange._2)

      // Generate the start timestamp and stop timestamp to run multi task for the query
      while (!needToRun(startTimestamp) ||
             queryHandler.completed(
               partitionKey(config.dbTableName, firstTimestampValue, startTimestamp),
               startTimestamp,
               stopTimestamp
             )) {
        val currentTimestamp = queryHandler.current()
        val timestampRange   = calcTimestampRange(firstTimestampValue, stopTimestamp)

        if (timestampRange._2.getTime <= currentTimestamp.getTime) {
          startTimestamp = timestampRange._1
          stopTimestamp = timestampRange._2
        } else if (needToRun(currentTimestamp))
          return queryHandler
            .queryData(
              partitionKey(config.dbTableName, firstTimestampValue, stopTimestamp),
              stopTimestamp,
              currentTimestamp
            )
            .asJava
        else return Seq.empty.asJava
      }
      val queryResult = queryHandler
        .queryData(partitionKey(config.dbTableName, firstTimestampValue, startTimestamp), startTimestamp, stopTimestamp)
      if (queryResult.nonEmpty) return queryResult.asJava
    } while (sleeper.tryToSleep())
    Seq.empty.asJava
  }

  override protected[source] def terminate(): Unit = Releasable.close(queryHandler)

  private[this] def replaceToCurrentTimestamp(timestamp: Timestamp): Timestamp = {
    val currentTimestamp = queryHandler.current()
    if (timestamp.getTime > currentTimestamp.getTime) currentTimestamp
    else timestamp
  }

  private[source] def needToRun(timestamp: Timestamp): Boolean = {
    val partitionHashCode = partitionKey(config.dbTableName, firstTimestampValue, timestamp).hashCode()
    Math.abs(partitionHashCode) % config.taskTotal == config.taskHash
  }

  private[source] def calcTimestampRange(
    firstTimestampValue: Timestamp,
    timestamp: Timestamp
  ): (Timestamp, Timestamp) = {
    if (timestamp.getTime < firstTimestampValue.getTime)
      throw new IllegalArgumentException("The timestamp less than the first data timestamp")
    val page             = (timestamp.getTime - firstTimestampValue.getTime) / TIMESTAMP_PARTITION_RANGE
    val startTimestamp   = new Timestamp((page * TIMESTAMP_PARTITION_RANGE) + firstTimestampValue.getTime)
    val stopTimestamp    = new Timestamp(startTimestamp.getTime + TIMESTAMP_PARTITION_RANGE)
    val currentTimestamp = queryHandler.current()
    if (startTimestamp.getTime > currentTimestamp.getTime && stopTimestamp.getTime > currentTimestamp.getTime)
      throw new IllegalArgumentException("The timestamp over the current timestamp")
    (startTimestamp, stopTimestamp)
  }

  private[source] def partitionKey(tableName: String, firstTimestampValue: Timestamp, timestamp: Timestamp): String = {
    val timestampRange = calcTimestampRange(firstTimestampValue, timestamp)
    s"$tableName:${timestampRange._1.toString}~${timestampRange._2.toString}"
  }
}
