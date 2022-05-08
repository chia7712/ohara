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

import java.sql.ResultSet

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers._

class TestMySQLDataTypeConverter extends OharaTest {
  @Test
  def testConverterBitValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBoolean("column1")).thenReturn(true)
    val column                 = RdbColumn("column1", "bit", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result shouldBe true
    result.isInstanceOf[Boolean] shouldBe true
  }

  @Test
  def testConverterTinyIntValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getInt("column1")).thenReturn(123)
    val column                 = RdbColumn("column1", "TINYINT", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result shouldBe 123
    result.isInstanceOf[Integer] shouldBe true
  }

  @Test
  def testConverterBoolValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBoolean("column1")).thenReturn(false)
    val column                 = RdbColumn("column1", "bool", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result shouldBe false
    result.isInstanceOf[Boolean] shouldBe true
  }

  @Test
  def testConverterSmallIntValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getInt("column1")).thenReturn(111)
    val column                 = RdbColumn("column1", "smallint", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result shouldBe 111
    result.isInstanceOf[Integer] shouldBe true
  }

  @Test
  def testConverterBigDecimalValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBigDecimal("column1")).thenReturn(java.math.BigDecimal.valueOf(1000L))
    val column                 = RdbColumn("column1", "decimal", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result.asInstanceOf[java.math.BigDecimal].intValue() shouldBe 1000
    result.isInstanceOf[java.math.BigDecimal] shouldBe true
  }

  @Test
  def testConverterBigDecimalNullValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBigDecimal("column1")).thenReturn(null)
    val column                 = RdbColumn("column1", "DECIMAL", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result.asInstanceOf[java.math.BigDecimal].intValue shouldBe 0
    result.isInstanceOf[java.math.BigDecimal] shouldBe true
  }

  @Test
  def testConverterVarCharValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getString("column1")).thenReturn("aaa")
    val column                 = RdbColumn("column1", "varchar", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result shouldBe "aaa"
    result.isInstanceOf[String] shouldBe true
  }

  @Test
  def testConverterVarCharNullValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getString("column1")).thenReturn(null)
    val column                 = RdbColumn("column1", "VARCHAR", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    result shouldBe "null"
    result.isInstanceOf[String] shouldBe true
  }

  @Test
  def testConverterBinaryValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBytes("column1")).thenReturn("aaaa".getBytes)
    val column                 = RdbColumn("column1", "binary", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)

    new String(result.asInstanceOf[Array[java.lang.Byte]].map(x => Byte.unbox(x))) shouldBe "aaaa"
    result.isInstanceOf[Array[java.lang.Byte]] shouldBe true
  }

  @Test
  def testConverterBinaryNullValue(): Unit = {
    val resultSet: ResultSet = Mockito.mock(classOf[ResultSet])
    when(resultSet.getBytes("column1")).thenReturn(null)
    val column                 = RdbColumn("column1", "BINARY", false)
    val mySQLDataTypeConverter = new MySQLDataTypeConverter()
    val result                 = mySQLDataTypeConverter.converterValue(resultSet, column)
    new String(result.asInstanceOf[Array[java.lang.Byte]].map(x => Byte.unbox(x))) shouldBe ""
    result.isInstanceOf[Array[java.lang.Byte]] shouldBe true
  }
}
