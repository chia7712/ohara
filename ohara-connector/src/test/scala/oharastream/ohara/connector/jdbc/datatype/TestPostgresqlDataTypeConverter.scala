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

import java.sql.{ResultSet, Time, Timestamp}

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.connector.jdbc.util.DateTimeUtils
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers._

class TestPostgresqlDataTypeConverter extends OharaTest {
  private[this] val BOOLEAN: String   = "BOOL"
  private[this] val BIT: String       = "BIT"
  private[this] val INT: String       = "INT4"
  private[this] val BPCHAR: String    = "BPCHAR"
  private[this] val VARCHAR: String   = "VARCHAR"
  private[this] val TIMESTAMP: String = "TIMESTAMP"

  @Test
  def testConverterBooleanValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBoolean("column1")).thenReturn(true)
    val column                                     = RdbColumn("column1", BOOLEAN, false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Boolean] shouldBe true
    result shouldBe true
  }

  @Test
  def testConverterBitValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    val value: Byte          = 1
    when(resultSet.getByte("column1")).thenReturn(value)
    val column                                     = RdbColumn("column1", BIT, false)
    val rDBDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rDBDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Boolean] shouldBe true
    result.isInstanceOf[Object] shouldBe true

    intercept[TestFailedException] {
      result.isInstanceOf[String] shouldBe true
    }.getMessage() shouldBe "false was not equal to true"
  }

  @Test
  def testConverterIntegerValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getInt("column1")).thenReturn(100)
    val column                                     = RdbColumn("column1", INT, false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Integer] shouldBe true
    result shouldBe 100
  }

  @Test
  def testConverterCharValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getString("column1")).thenReturn("h")
    val column                                     = RdbColumn("column1", BPCHAR, false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[String] shouldBe true
    result shouldBe "h"
  }

  @Test
  def testConveterTimestampValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getTimestamp("column1", DateTimeUtils.CALENDAR)).thenReturn(new Timestamp(0L))
    val column                                     = RdbColumn("column1", TIMESTAMP, false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Timestamp] shouldBe true
    result.isInstanceOf[Object] shouldBe true
    result.toString shouldBe "1970-01-01 08:00:00.0"
  }

  @Test
  def testConverterVarcharValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getString("column1")).thenReturn("hello")
    val column                                     = RdbColumn("column1", VARCHAR, false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[String] shouldBe true
    result shouldBe "hello"
  }

  @Test
  def testConverterByteaValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBytes("column1")).thenReturn("aaa".getBytes)
    val column                                     = RdbColumn("column1", "BYTEA", false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Array[java.lang.Byte]] shouldBe true
    new String(result.asInstanceOf[Array[java.lang.Byte]].map(x => Byte.unbox(x))) shouldBe "aaa"
  }

  @Test
  def testConverterTimeValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getTime("column1", DateTimeUtils.CALENDAR)).thenReturn(Time.valueOf("11:00:00"))
    val column                                     = RdbColumn("column1", "TIME", false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    val result: Any                                = rdbDataTypeConverter.converterValue(resultSet, column)
    result.isInstanceOf[Time] shouldBe true
    result.asInstanceOf[Time].toString shouldBe "11:00:00"
  }

  @Test
  def testErrorDataType(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getString("column1")).thenReturn("aaa")
    val column                                     = RdbColumn("column1", "AAA", false)
    val rdbDataTypeConverter: RDBDataTypeConverter = new PostgresqlDataTypeConverter()
    an[UnsupportedOperationException] should be thrownBy
      rdbDataTypeConverter.converterValue(resultSet, column)
  }
}
