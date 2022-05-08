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

package oharastream.ohara.connector.perf
import java.util.concurrent.TimeUnit

import oharastream.ohara.common.annotations.VisibleForTesting
import oharastream.ohara.common.data._
import oharastream.ohara.common.setting.TopicKey
import oharastream.ohara.common.util.{ByteUtils, CommonUtils}
import oharastream.ohara.kafka.connector.{RowSourceRecord, RowSourceTask, TaskSetting}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class PerfSourceTask extends RowSourceTask {
  private[this] var props: PerfSourceProps = _
  private[this] var topics: Set[TopicKey]  = _
  @VisibleForTesting
  private[perf] var schema: Seq[Column] = _
  private[this] var lastPoll: Long      = -1

  /**
    * this is what we push to topics. We don't generate it repeatedly to avoid extra cost in testing.
    */
  private[this] var records: java.util.List[RowSourceRecord] = java.util.List.of()

  /**
    * convert object to byte array is expensive so we keep all results.
    */
  private[this] val rowCache = mutable.Map[Row, Array[Byte]]()

  override protected def run(settings: TaskSetting): Unit = {
    this.props = PerfSourceProps(settings)
    this.topics = settings.topicKeys().asScala.toSet
    this.schema = settings.columns.asScala.toSeq
    if (schema.isEmpty) schema = PerfSourceTask.DEFAULT_SCHEMA
    (0 until props.batch).foreach { index =>
      val value = CommonUtils.current() + index
      val row = Row.of(
        schema.sortBy(_.order).map { c =>
          Cell.of(
            c.newName,
            convertToValue(c.dataType, value)
          )
        }: _*
      )
      rowCache.put(row, Serializer.ROW.to(row))
    }
    records = rowCache.keys
      .flatMap(row => topics.map(RowSourceRecord.builder().row(row).topicKey(_).build()))
      .toSeq
      .asJava
  }

  override protected def toBytes(record: RowSourceRecord): Array[Byte] =
    rowCache.getOrElse(record.row(), Serializer.ROW.to(record.row()))

  override protected def terminate(): Unit = {}

  override protected def pollRecords(): java.util.List[RowSourceRecord] = {
    val timeToWait = lastPoll + props.freq.toMillis - CommonUtils.current()
    if (timeToWait > 0) TimeUnit.MILLISECONDS.sleep(timeToWait)
    lastPoll = CommonUtils.current()
    records
  }

  private[perf] def convertToValue(dataType: DataType, value: Long): Any = {
    dataType match {
      case DataType.BOOLEAN => java.lang.Boolean.valueOf(false)
      case DataType.BYTE    => java.lang.Byte.valueOf(ByteUtils.toBytes(value).head)
      case DataType.BYTES   => BigInt(value).toByteArray.map(x => java.lang.Byte.valueOf(x))
      case DataType.SHORT   => java.lang.Short.valueOf(value.toShort)
      case DataType.INT     => java.lang.Integer.valueOf(value.toInt)
      case DataType.LONG    => java.lang.Long.valueOf(value)
      case DataType.FLOAT   => java.lang.Float.valueOf(value.toFloat)
      case DataType.DOUBLE  => java.lang.Double.valueOf(value.toDouble)
      case DataType.STRING  => java.lang.String.valueOf(CommonUtils.randomString(props.cellSize))
      case _                => value
    }
  }
}

object PerfSourceTask {
  private[perf] val DEFAULT_SCHEMA = Seq(
    Column.builder().name("a").dataType(DataType.STRING).order(0).build(),
    Column.builder().name("b").dataType(DataType.STRING).order(1).build(),
    Column.builder().name("c").dataType(DataType.STRING).order(2).build()
  )
}
