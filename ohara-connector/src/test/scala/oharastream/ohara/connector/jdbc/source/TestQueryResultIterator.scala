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

import java.sql.{PreparedStatement, ResultSet}

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.connector.jdbc.datatype.RDBDataTypeConverter
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers._

class TestQueryResultIterator extends OharaTest {
  private[this] val VARCHAR: String = "VARCHAR"

  @Test
  def testOnlyNext(): Unit = {
    val preparedStatement = Mockito.mock(classOf[PreparedStatement])
    val resultSet         = Mockito.mock(classOf[ResultSet])
    when(preparedStatement.executeQuery()).thenReturn(resultSet)

    val columnList = Seq(
      RdbColumn("column1", VARCHAR, false),
      RdbColumn("column2", VARCHAR, false),
      RdbColumn("column3", VARCHAR, false)
    )

    val dataTypeConverter: RDBDataTypeConverter = Mockito.mock(classOf[RDBDataTypeConverter])
    val it: Iterator[Seq[Object]]               = new QueryResultIterator(dataTypeConverter, resultSet, columnList)
    intercept[NoSuchElementException] {
      it.next()
    }.getMessage shouldBe "Cache no data"
  }

  @Test
  def testHasNextHaveData(): Unit = {
    val preparedStatement = Mockito.mock(classOf[PreparedStatement])
    val resultSet         = Mockito.mock(classOf[ResultSet])
    when(preparedStatement.executeQuery()).thenReturn(resultSet)
    when(resultSet.next()).thenReturn(true).thenReturn(false)
    when(resultSet.getString("column1")).thenReturn("value1-1")
    when(resultSet.getString("column2")).thenReturn("value1-2")
    when(resultSet.getString("column3")).thenReturn("value1-3")

    val columnList = Seq(
      RdbColumn("column1", VARCHAR, false),
      RdbColumn("column2", VARCHAR, false),
      RdbColumn("column3", VARCHAR, false)
    )

    val dataTypeConverter: RDBDataTypeConverter = Mockito.mock(classOf[RDBDataTypeConverter])
    val it: Iterator[Seq[Object]]               = new QueryResultIterator(dataTypeConverter, resultSet, columnList)
    var count: Int                              = 0
    while (it.hasNext) {
      it.next()
      count = count + 1
    }
    count shouldBe 1
  }

  @Test
  def testHasNextHaveMoreData(): Unit = {
    val preparedStatement = Mockito.mock(classOf[PreparedStatement])
    val resultSet         = Mockito.mock(classOf[ResultSet])
    when(preparedStatement.executeQuery()).thenReturn(resultSet)
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false)
    when(resultSet.getString("column1")).thenReturn("value1-1").thenReturn("value2-1").thenReturn("value3-1")
    when(resultSet.getString("column2")).thenReturn("value1-2").thenReturn("value2-2").thenReturn("value2-3")
    when(resultSet.getString("column3")).thenReturn("value1-3").thenReturn("value2-3").thenReturn("value3-3")

    val columnList = Seq(
      RdbColumn("column1", VARCHAR, false),
      RdbColumn("column2", VARCHAR, false),
      RdbColumn("column3", VARCHAR, false)
    )

    val dataTypeConverter: RDBDataTypeConverter = Mockito.mock(classOf[RDBDataTypeConverter])
    val it: Iterator[Seq[Object]]               = new QueryResultIterator(dataTypeConverter, resultSet, columnList)
    var count: Int                              = 0
    while (it.hasNext) {
      it.next()
      count = count + 1
    }
    count shouldBe 3
  }

  @Test
  def testHasNextNoData(): Unit = {
    val preparedStatement = Mockito.mock(classOf[PreparedStatement])
    val resultSet         = Mockito.mock(classOf[ResultSet])
    when(preparedStatement.executeQuery()).thenReturn(resultSet)
    when(resultSet.next()).thenReturn(false)

    val columnList = Seq(
      RdbColumn("column1", VARCHAR, false),
      RdbColumn("column2", VARCHAR, false),
      RdbColumn("column3", VARCHAR, false)
    )

    val dataTypeConverter: RDBDataTypeConverter = Mockito.mock(classOf[RDBDataTypeConverter])
    val it: Iterator[Seq[Object]]               = new QueryResultIterator(dataTypeConverter, resultSet, columnList)
    var count: Int                              = 0
    while (it.hasNext) {
      it.next()
      count = count + 1
    }
    count shouldBe 0
  }
}
