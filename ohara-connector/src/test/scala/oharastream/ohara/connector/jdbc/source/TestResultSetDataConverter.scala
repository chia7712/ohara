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

import java.sql.{ResultSet, Time, Timestamp}

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.connector.jdbc.datatype.{MySQLDataTypeConverter, RDBDataTypeConverter}
import oharastream.ohara.connector.jdbc.util.{ColumnInfo, DateTimeUtils}
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers._

class TestResultSetDataConverter extends OharaTest {
  private[this] val VARCHAR: String   = "VARCHAR"
  private[this] val TIMESTAMP: String = "TIMESTAMP"
  private[this] val INT: String       = "INT"
  private[this] val DATE: String      = "DATE"
  private[this] val TIME: String      = "TIME"

  @Test
  def testConverterRecord(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getTimestamp("column1", DateTimeUtils.CALENDAR)).thenReturn(new Timestamp(0L))
    when(resultSet.getString("column2")).thenReturn("aaa")
    when(resultSet.getInt("column3")).thenReturn(10)

    val columnList = Seq(
      RdbColumn("column1", TIMESTAMP, true),
      RdbColumn("column2", VARCHAR, false),
      RdbColumn("column3", INT, false)
    )
    val dataTypeConverter: RDBDataTypeConverter = new MySQLDataTypeConverter()
    val result: Seq[ColumnInfo[_]]              = ResultSetDataConverter.converterRecord(dataTypeConverter, resultSet, columnList)
    result.head.columnName shouldBe "column1"
    result.head.columnType shouldBe TIMESTAMP
    result.head.value.toString shouldBe "1970-01-01 08:00:00.0"

    result(1).columnName shouldBe "column2"
    result(1).columnType shouldBe VARCHAR
    result(1).value shouldBe "aaa"

    result(2).columnName shouldBe "column3"
    result(2).columnType shouldBe INT
    result(2).value shouldBe 10
  }

  @Test
  def testNullValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getTimestamp("column1", DateTimeUtils.CALENDAR)).thenReturn(new Timestamp(0L))
    when(resultSet.getString("column2")).thenReturn(null)
    when(resultSet.getDate("column3")).thenReturn(null)
    when(resultSet.getTime("column4")).thenReturn(null)

    val columnList = Seq(
      RdbColumn("column1", TIMESTAMP, true),
      RdbColumn("column2", VARCHAR, false),
      RdbColumn("column3", DATE, false),
      RdbColumn("column4", TIME, false)
    )
    val dataTypeConverter: RDBDataTypeConverter = new MySQLDataTypeConverter()
    val result: Seq[ColumnInfo[_]]              = ResultSetDataConverter.converterRecord(dataTypeConverter, resultSet, columnList)
    result(1).columnName shouldBe "column2"
    result(1).columnType shouldBe VARCHAR
    result(1).value shouldBe "null"

    result(2).columnName shouldBe "column3"
    result(2).columnType shouldBe DATE
    result(2).value.toString shouldBe "1970-01-01"

    result(3).columnName shouldBe "column4"
    result(3).columnType shouldBe TIME
    result(3).value.toString shouldBe new Time(0).toString
  }
}
