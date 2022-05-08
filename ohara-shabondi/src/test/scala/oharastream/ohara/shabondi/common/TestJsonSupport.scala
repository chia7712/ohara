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

package oharastream.ohara.shabondi.common

import oharastream.ohara.common.data.{Cell, Row}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json._

import scala.jdk.CollectionConverters._

final class TestJsonSupport extends OharaTest {
  @Test
  def testRowData(): Unit = {
    val jsonData =
      """
        |{"col1":"hello", "col2": 200}
        |""".stripMargin

    val rowData: JsonSupport.RowData = JsonSupport.rowDataFormat.read(jsonData.parseJson)

    rowData("col1") should ===(JsString("hello"))
    rowData("col2") should ===(JsNumber(200))

    val row = JsonSupport.toRow(rowData)

    row.cell(0) should ===(Cell.of("col1", "hello"))
    row.cell(1) should ===(Cell.of("col2", 200))
  }

  @Test
  def testConversion(): Unit = {
    val json =
      """
        |  {
        |    "a": "b",
        |    "b": 123,
        |    "c": false,
        |    "d": null,
        |    "e": [
        |      "a",
        |      "c"
        |    ],
        |    "f": [
        |      {
        |        "f0": "v",
        |        "f1": 123,
        |        "tags": []
        |      }
        |    ],
        |    "g": {
        |      "a": "c",
        |      "d": 123,
        |      "dd": true,
        |      "tags": []
        |    },
        |    "tags": []
        |  }
        |""".stripMargin.parseJson.asJsObject

    val row   = JsonSupport.toRow(json)
    val json2 = JsonSupport.toJson(row)
    JsObject(JsonSupport.noJsNull(json.fields)) shouldBe json2
  }

  @Test
  def testTags(): Unit = {
    val tags = Seq(CommonUtils.randomString(), CommonUtils.randomString())
    val row  = Row.of(tags.asJava, Cell.of("a", "b"))
    val json = JsonSupport.toJson(row)
    json.fields(JsonSupport.TAGS_KEY).asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value) shouldBe tags

    val row2 = JsonSupport.toRow(json)
    row2.tags().asScala shouldBe tags
  }

  @Test
  def testTimestamp(): Unit = {
    val key       = "a"
    val timestamp = new java.sql.Timestamp(System.currentTimeMillis())
    val row       = Row.of(Cell.of(key, timestamp))
    JsonSupport.toJson(row).fields(key).asInstanceOf[JsString].value shouldBe timestamp.toString
  }
}
