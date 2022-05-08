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

package oharastream.ohara.client.configurator

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.setting.SettingDef.Permission
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.DefaultJsonProtocol._
import spray.json.{RootJsonFormat, _}
class TestJsonRefiner extends OharaTest {
  private[this] implicit val format: RootJsonFormat[SimpleData] = jsonFormat6(SimpleData)
  private[this] val format2: RootJsonFormat[SimpleData2]        = jsonFormat2(SimpleData2)

  @Test
  def nullFormat(): Unit = an[NullPointerException] should be thrownBy JsonRefiner.builder[SimpleData].format(null)

  @Test
  def emptyConnectionPort(): Unit =
    an[IllegalArgumentException] should be thrownBy JsonRefiner.builder[SimpleData].requireConnectionPort("")

  @Test
  def emptyToNullToEmptyArray(): Unit =
    an[IllegalArgumentException] should be thrownBy JsonRefiner.builder[SimpleData].nullToEmptyArray("")

  @Test
  def testDuplicateKeyForFromAnotherKey(): Unit = {
    val actions: Seq[JsonRefinerBuilder[SimpleData] => Unit] = Seq(
      _.nullToAnotherValueOfKey("a", "b")
    )
    actions.foreach { action0 =>
      actions.foreach { action1 =>
        val refiner = JsonRefinerBuilder[SimpleData].format(format)
        action0(refiner)
        an[IllegalArgumentException] should be thrownBy action1(refiner)
      }
    }
  }

  @Test
  def testDuplicateKeyForDefaultValue(): Unit = {
    val actions: Seq[JsonRefinerBuilder[SimpleData] => Unit] = Seq(
      _.nullToShort("a", 1),
      _.nullToInt("a", 1),
      _.nullToLong("a", 1),
      _.nullToDouble("a", 1),
      _.nullToEmptyArray("a"),
      _.nullToString("a", "ccc")
    )
    actions.foreach { action0 =>
      actions.foreach { action1 =>
        val refiner = JsonRefinerBuilder[SimpleData].format(format)
        action0(refiner)
        an[IllegalArgumentException] should be thrownBy action1(refiner)
      }
    }
  }

  @Test
  def testDuplicateKeyForChecker(): Unit = {
    val actions: Seq[JsonRefinerBuilder[SimpleData] => Unit] = Seq(
      _.requireConnectionPort("a"),
      _.requireConnectionPort("a")
    )
    actions.foreach { action0 =>
      actions.foreach { action1 =>
        val refiner = JsonRefinerBuilder[SimpleData].format(format)
        // duplicate checks will be merge to single one
        action0(refiner)
        action1(refiner)
      }
    }
  }

  @Test
  def testEmptyString(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .build
      .read("""
            |{
            | "stringValue": "",
            | "bindPort": 123,
            | "connectionPort": 12144,
            | "stringArray": ["aa"],
            | "objects":{}
            |}
          """.stripMargin.parseJson)

  @Test
  def testConnectionPort(): Unit =
    JsonRefinerBuilder[SimpleData]
      .format(format)
      .requireConnectionPort("connectionPort")
      .build
      .read("""
              |{
              | "stringValue": "abc",
              | "group": "default",
              | "bindPort": 123,
              | "connectionPort": 77,
              | "stringArray": ["aa"],
              | "objects":{}
              |}
            """.stripMargin.parseJson)
      .connectionPort shouldBe 77

  @Test
  def testNullConnectionPort(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .requireConnectionPort("connectionPort")
      .build
      .read("""
            |{
            | "stringValue": "abc",
            | "bindPort": 123,
            | "connectionPort": null,
            | "stringArray": ["aa"],
            | "objects":{}
            |}
          """.stripMargin.parseJson)

  @Test
  def testIgnoreConnectionPort(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .requireConnectionPort("connectionPort")
      .build
      .read("""
            |{
            | "stringValue": "abc",
            | "bindPort": 123,
            | "stringArray": ["aa"],
            | "objects":{}
            |}
          """.stripMargin.parseJson)

  @Test
  def testNegativeConnectionPort(): Unit = testIllegalConnectionPort(-1)

  @Test
  def testZeroConnectionPort(): Unit = testIllegalConnectionPort(0)

  @Test
  def testLargeConnectionPort(): Unit = testIllegalConnectionPort(1000000)

  private[this] def testIllegalConnectionPort(port: Int): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .requireConnectionPort("connectionPort")
      .build
      .read(s"""
              |{
              | "stringValue": "abc",
              | "bindPort": 123,
              | "connectionPort": $port,
              | "stringArray": ["aa"],
              | "objects":{}
              |}
            """.stripMargin.parseJson)

  @Test
  def testNegativeConnectionPortWithoutCheck(): Unit =
    JsonRefinerBuilder[SimpleData].format(format).build.read("""
              |{
              | "stringValue": "abc",
              | "group": "default",
              | "bindPort": 123,
              | "connectionPort": -1,
              | "stringArray": ["aa"],
              | "objects":{}
              |}
            """.stripMargin.parseJson).connectionPort shouldBe -1

  @Test
  def testNegativeBindPortWithoutCheck(): Unit =
    JsonRefinerBuilder[SimpleData].format(format).build.read("""
                     |{
                     | "stringValue": "abc",
                     | "group": "default",
                     | "bindPort": -1,
                     | "connectionPort": 123,
                     | "stringArray": ["aa"],
                     | "objects":{}
                     |}
                   """.stripMargin.parseJson).bindPort shouldBe -1

  @Test
  def testNullToEmptyArray(): Unit =
    JsonRefinerBuilder[SimpleData].format(format).nullToEmptyArray("stringArray").build.read("""
             |{
             | "stringValue": "abc",
             | "group": "default",
             | "bindPort": -1,
             | "connectionPort": 123,
             | "objects":{}
             |}
           """.stripMargin.parseJson).stringArray shouldBe Seq.empty

  @Test
  def defaultInt(): Unit =
    JsonRefinerBuilder[SimpleData].format(format).nullToInt("bindPort", 777).build.read("""
         |{
         | "stringValue": "abc",
         | "group": "default",
         | "connectionPort": 123,
         | "stringArray": [],
         | "objects":{}
         |}
       """.stripMargin.parseJson).bindPort shouldBe 777

  @Test
  def testNullStringInDefaultToAnother(): Unit = {
    an[NullPointerException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .nullToAnotherValueOfKey(null, CommonUtils.randomString())
    an[NullPointerException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .nullToAnotherValueOfKey(CommonUtils.randomString(), null)
  }

  @Test
  def testEmptyStringInDefaultToAnother(): Unit = {
    an[IllegalArgumentException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .nullToAnotherValueOfKey("", CommonUtils.randomString())
    an[IllegalArgumentException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .nullToAnotherValueOfKey(CommonUtils.randomString(), "")
  }

  @Test
  def testDefaultToAnother(): Unit =
    JsonRefinerBuilder[SimpleData]
      .format(format)
      .nullToAnotherValueOfKey("bindPort", "connectionPort")
      .build
      .read("""
        |{
        | "stringValue": "abc",
        | "group": "default",
        | "connectionPort": 123,
        | "stringArray": [],
        | "objects":{}
        |}
      """.stripMargin.parseJson)
      .bindPort shouldBe 123

  @Test
  def testNonexistentAnotherKeyForDefaultToAnother(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .nullToAnotherValueOfKey("bindPort", CommonUtils.randomString())
      .build
      .read("""
      |{
      | "stringValue": "abc",
      | "connectionPort": 123,
      | "stringArray": [],
      | "objects":{}
      |}
    """.stripMargin.parseJson)

  /**
    * JsonRefiner doesn't another key if the origin key exists!!!
    */
  @Test
  def testNonexistentAnotherKeyButOriginKeyExistForDefaultToAnother(): Unit =
    JsonRefinerBuilder[SimpleData]
      .format(format)
      .nullToAnotherValueOfKey("bindPort", CommonUtils.randomString())
      .build
      .read("""
              |{
              | "stringValue": "abc",
              | "group": "default",
              | "bindPort": 9999,
              | "connectionPort": 123,
              | "stringArray": [],
              | "objects":{}
              |}
            """.stripMargin.parseJson)
      .bindPort shouldBe 9999

  @Test
  def testNestedObjectForEmptyString(): Unit =
    JsonRefinerBuilder[SimpleData2].format(format2).build.read("""
            |{
            |  "data": {
            |    "stringValue": "abc",
            |    "group": "default",
            |    "bindPort": 22,
            |    "connectionPort": 123,
            |    "stringArray": [],
            |    "objects":{}
            |  },
            |  "data2": [
            |    {
            |      "stringValue": "abc",
            |      "group": "default",
            |      "bindPort": 22,
            |      "connectionPort": 123,
            |      "stringArray": [],
            |      "objects":{}
            |    }
            |  ]
            |
            |}
          """.stripMargin.parseJson)

  @Test
  def testNestedObjectForEmptyStringWithEmptyInFirstElement(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData2]
      .format(format2)
      .build
      .read("""
            |{
            |  "data": {
            |    "stringValue": "",
            |    "group": "default",
            |    "bindPort": 22,
            |    "connectionPort": 123,
            |    "stringArray": [],
            |    "objects":{}
            |  },
            |  "data2": [
            |    {
            |      "stringValue": "abc",
            |      "bindPort": 22,
            |      "connectionPort": 123,
            |      "stringArray": [],
            |      "objects":{}
            |    }
            |  ]
            |
            |}
          """.stripMargin.parseJson)

  @Test
  def testNestedObjectForEmptyStringWithEmptyInSecondElement(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData2]
      .format(format2)
      .build
      .read("""
            |{
            |  "data": {
            |    "stringValue": "aaa",
            |    "bindPort": 22,
            |    "connectionPort": 123,
            |    "stringArray": [],
            |    "objects":{}
            |  },
            |  "data2": [
            |    {
            |      "stringValue": "",
            |      "bindPort": 22,
            |      "connectionPort": 123,
            |      "stringArray": [],
            |      "objects":{}
            |    }
            |  ]
            |
            |}
          """.stripMargin.parseJson)

  @Test
  def testRejectEmptyArrayForSpecificKey(): Unit = {
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      // connectionPort is not mapped to array type so it is rejected
      .rejectEmptyArray("connectionPort")
      .build
      .read("""
              |{
              | "stringValue": "abc",
              | "bindPort": 9999,
              | "connectionPort": 123,
              | "stringArray": [],
              | "objects":{}
              |}
            """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .rejectEmptyArray("stringArray")
      .build
      .read("""
              |{
              | "stringValue": "abc",
              | "bindPort": 9999,
              | "connectionPort": 123,
              | "stringArray": [],
              | "objects":{}
              |}
            """.stripMargin.parseJson)
  }

  @Test
  def testAcceptStringToNumber(): Unit = {
    val bindPort       = CommonUtils.availablePort()
    val connectionPort = CommonUtils.availablePort()
    val data = JsonRefinerBuilder[SimpleData]
      .format(format)
      .acceptStringToNumber("bindPort")
      .acceptStringToNumber("connectionPort")
      .build
      .read(s"""
              |{
              | "stringValue": "abc",
              | "group": "default",
              | "bindPort": "$bindPort",
              | "connectionPort": "$connectionPort",
              | "stringArray": [],
              | "objects":{}
              |}
            """.stripMargin.parseJson)

    data.bindPort shouldBe bindPort
    data.connectionPort shouldBe connectionPort
  }

  @Test
  def testParseStringForBindPOrt(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .build
      .read("""
            |{
            | "stringValue": "abc",
            | "bindPort": "123",
            | "connectionPort": 123,
            | "stringArray": [],
            | "objects":{}
            |}
          """.stripMargin.parseJson)

  @Test
  def testParseStringForConnectionPOrt(): Unit =
    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .build
      .read("""
            |{
            | "stringValue": "abc",
            | "bindPort": 123,
            | "connectionPort": "123",
            | "stringArray": [],
            | "objects":{}
            |}
          """.stripMargin.parseJson)

  @Test
  def nullToAnotherValueOfKeyShouldBeBeforeNullToEmptyArray(): Unit =
    JsonRefinerBuilder[SimpleData]
      .format(format)
      .nullToAnotherValueOfKey("stringArray", "ttt")
      .nullToEmptyArray("stringArray")
      .build
      .read("""
              |{
              | "stringValue": "abc",
              | "group": "default",
              | "bindPort": 123,
              | "connectionPort": 111,
              | "ttt": ["abc"],
              | "objects":{}
              |}
            """.stripMargin.parseJson)
      .stringArray shouldBe Seq("abc")

  @Test
  def emptyArrayInMappingToAnotherNonexistentKey(): Unit =
    JsonRefinerBuilder[SimpleData]
      .format(format)
      .nullToAnotherValueOfKey("stringArray", "ttt")
      .nullToEmptyArray("stringArray")
      .build
      .read("""
              |{
              | "stringValue": "abc",
              | "group": "default",
              | "bindPort": 123,
              | "connectionPort": 111,
              | "objects":{}
              |}
            """.stripMargin.parseJson)
      .stringArray shouldBe Seq.empty

  @Test
  def nullToEmptyObject(): Unit =
    JsonRefinerBuilder[SimpleData].format(format).nullToEmptyObject("objects").build.read(s"""
             |{
             | "stringValue": "111",
             | "group": "default",
             | "bindPort": 123,
             | "connectionPort": 111,
             | "stringArray": []
             |}
           """.stripMargin.parseJson).objects shouldBe Map.empty

  @Test
  def testObjects(): Unit = JsonRefinerBuilder[SimpleData].format(format).build.read(s"""
       |{
       | "stringValue": "111",
       | "group": "default",
       | "bindPort": 123,
       | "connectionPort": 111,
       | "stringArray": [],
       | "objects": {
       |   "a": "bb",
       |   "b": 123
       | }
       |}
           """.stripMargin.parseJson).objects shouldBe Map("a" -> JsString("bb"), "b" -> JsNumber(123))

  @Test
  def testKeywordsInArray(): Unit = {
    // pass
    JsonRefinerBuilder[SimpleData]
      .format(format)
      .rejectKeywordsFromArray("stringArray", Set("start"))
      .build
      .read(s"""
                    |{
                    | "stringValue": "aaa",
                    | "group": "default",
                    | "bindPort": 123,
                    | "connectionPort": 111,
                    | "stringArray": ["ss", "tt"],
                    | "objects":{}
                    |}
                 """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy JsonRefinerBuilder[SimpleData]
      .format(format)
      .rejectKeywordsFromArray("stringArray", Set("stop"))
      .build
      .read(s"""
               |{
               | "stringValue": "start",
               | "bindPort": 123,
               | "connectionPort": 111,
               | "stringArray": ["stop", "abc"],
               | "objects":{}
               |}
             """.stripMargin.parseJson)
  }

  @Test
  def testRequireKeys(): Unit = {
    val data = SimpleData(
      stringValue = CommonUtils.randomString(),
      group = CommonUtils.randomString(),
      bindPort = CommonUtils.availablePort(),
      connectionPort = CommonUtils.availablePort(),
      stringArray = Seq.empty,
      objects = Map.empty
    )

    // make sure the normal format works well
    format.read(format.write(data))

    val f = JsonRefinerBuilder[SimpleData].format(format).requireKeys(Set("a", "b")).build
    an[DeserializationException] should be thrownBy f.read(format.write(data))

    an[DeserializationException] should be thrownBy f.read(
      JsObject(
        format.write(data).asJsObject.fields + ("a" -> JsString("bb"))
      )
    )

    f.read(
      JsObject(
        format.write(data).asJsObject.fields + ("a" -> JsString("bb")) + ("b" -> JsString("bb"))
      )
    )
  }

  @Test
  def testNumberRange(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .requireNumberType(key, 0, 100)
      .build

    format.read(s"""
                   |  {
                   |    "$key": 50
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(50)

    an[DeserializationException] should be thrownBy format.read(s"""
         |  {
         |    "$key": -1
         |  }
         |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 999
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)
  }

  @Test
  def testStringDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.STRING).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": "50"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsString("50")

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 123
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "asdasdsad": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)
  }

  @Test
  def testStringDefinitionWithDefaultValue(): Unit = {
    val key     = CommonUtils.randomString()
    val default = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).optional(default).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": "50"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsString("50")

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 123
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    format.read(s"""
                   |  {
                   |    "asdasdsad": "a"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsString(default)
  }

  @Test
  def testShortDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.SHORT).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": 50
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(50)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": ${Long.MaxValue}
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": ${Long.MinValue}
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "asdasdsad": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)
  }

  @Test
  def testShortDefinitionWithDefaultValue(): Unit = {
    val key            = CommonUtils.randomString()
    val default: Short = 100
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).optional(default).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": 50
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(50)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": ${Long.MaxValue}
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": ${Long.MinValue}
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    format.read(s"""
                   |  {
                   |    "asdasdsad": "a"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(default)
  }

  @Test
  def testObjectKeyDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.OBJECT_KEY).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": {
                   |      "group": "g",
                   |      "name": "n"
                   |    }
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsObject(
      Map("group" -> JsString("g"), "name" -> JsString("n"))
    )

    // this form is ok to ObjectKey - the default value of group is "default"
    format.read(s"""
                   |  {
                   |    "$key": "a"
                   |  }
                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": {
                                                                   |      "b": "b"
                                                                   |    }
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "asdasdsad": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)
  }

  @Test
  def testTagsDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.TAGS).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": {
                   |      "group": "g",
                   |      "name": "n"
                   |    }
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsObject(
      Map("group" -> JsString("g"), "name" -> JsString("n"))
    )

    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "$key": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("must be JsObject type")

    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "asdasdsad": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("empty")
  }

  @Test
  def testObjectKeysDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.OBJECT_KEYS).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": [
                   |      {
                   |        "group": "g",
                   |        "name": "n"
                   |      }
                   |    ]
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsArray(
      Vector(JsObject(Map("group" -> JsString("g"), "name" -> JsString("n"))))
    )

    // this error is generated by akka so the error message is a bit different.
    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "$key": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("but got")

    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "asdasdsad": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("empty")
  }

  @Test
  def testArrayDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.ARRAY).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": ["a", "b"]
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsArray(Vector(JsString("a"), JsString("b")))

    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "$key": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("must be JsArray type")

    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "asdasdsad": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("empty")
  }

  @Test
  def testPortDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.REMOTE_PORT).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": 2222
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(2222)

    format.read(s"""
                   |  {
                   |    "$key": 1
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(1)

    format.read(s"""
                   |  {
                   |    "$key": 65535
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(65535)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": -1
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 0
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 65536
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "asdasdsad": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)
  }

  @Test
  def testBindingPortDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.BINDING_PORT).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": 2222
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(2222)

    format.read(s"""
                   |  {
                   |    "$key": 1
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(1)

    format.read(s"""
                   |  {
                   |    "$key": 65535
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(65535)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": -1
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 0
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": 65536
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "$key": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)

    an[DeserializationException] should be thrownBy format.read(s"""
                                                                   |  {
                                                                   |    "asdasdsad": "a"
                                                                   |  }
                                                                   |  """.stripMargin.parseJson)
  }

  @Test
  def testTableDefinitionWithoutDefaultValue(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.TABLE).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": [
                   |      {
                   |        "a": "b",
                   |        "b": "c"
                   |      },
                   |      {
                   |        "a1": "b",
                   |        "b1": "c"
                   |      }
                   |    ]
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsArray(
      Vector(
        JsObject(Map("a"  -> JsString("b"), "b"  -> JsString("c"))),
        JsObject(Map("a1" -> JsString("b"), "b1" -> JsString("c")))
      )
    )

    // the error is generated by jackson so the error message is a bit different
    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "$key": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("failed to convert")

    intercept[DeserializationException] {
      format.read(s"""
                     |  {
                     |    "asdasdsad": "a"
                     |  }
                     |  """.stripMargin.parseJson)
    }.getMessage should include("empty")
  }

  @Test
  def testNumber(): Unit = {
    val key = CommonUtils.randomString()

    val types = Seq(SettingDef.Type.SHORT, SettingDef.Type.INT, SettingDef.Type.LONG, SettingDef.Type.DOUBLE)

    types.foreach { t =>
      val format = JsonRefinerBuilder[JsObject]
        .format(new RootJsonFormat[JsObject] {
          override def read(json: JsValue): JsObject = json.asJsObject
          override def write(obj: JsObject): JsValue = obj
        })
        .definition(SettingDef.builder().key(key).required(t).build())
        .build

      val nonPositiveNumber = Seq(Short.MinValue, -1, 0)

      nonPositiveNumber.foreach { illegal =>
        format.read(s"""
           |  {
           |    "$key": $illegal
           |  }
           |  """.stripMargin.parseJson)
      }

      format.read(s"""
                     |  {
                     |    "$key": 100
                     |  }
                     |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(100)
    }
  }
  @Test
  def testPositiveNumber(): Unit = {
    val key = CommonUtils.randomString()

    val types = Seq(
      SettingDef.Type.POSITIVE_SHORT,
      SettingDef.Type.POSITIVE_INT,
      SettingDef.Type.POSITIVE_LONG,
      SettingDef.Type.POSITIVE_DOUBLE
    )

    types.foreach { t =>
      val format = JsonRefinerBuilder[JsObject]
        .format(new RootJsonFormat[JsObject] {
          override def read(json: JsValue): JsObject = json.asJsObject
          override def write(obj: JsObject): JsValue = obj
        })
        .definition(SettingDef.builder().key(key).required(t).build())
        .build

      val nonPositiveNumber = Seq(Short.MinValue, -1, 0)

      nonPositiveNumber.foreach { illegal =>
        an[DeserializationException] should be thrownBy format.read(s"""
                                                                         |  {
                                                                         |    "$key": $illegal
                                                                         |  }
                                                                         |  """.stripMargin.parseJson)
      }

      format.read(s"""
                     |  {
                     |    "$key": 100
                     |  }
                     |  """.stripMargin.parseJson).fields(key) shouldBe JsNumber(100)
    }
  }

  @Test
  def testCompleteObjectKey(): Unit = {
    val key  = CommonUtils.randomString()
    val name = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.OBJECT_KEY).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": "$name"
                   |  }
                   |  """.stripMargin.parseJson).fields(key).asJsObject.fields(GROUP_KEY) shouldBe JsString(
      GROUP_DEFAULT
    )

    format.read(s"""
                   |  {
                   |    "$key": {
                   |      "name": "$name"
                   |    }
                   |  }
                   |  """.stripMargin.parseJson).fields(key).asJsObject.fields(GROUP_KEY) shouldBe JsString(
      GROUP_DEFAULT
    )
  }

  @Test
  def testCompleteObjectKeys(): Unit = {
    val key   = CommonUtils.randomString()
    val name  = CommonUtils.randomString()
    val name2 = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject
        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).required(SettingDef.Type.OBJECT_KEYS).build())
      .build

    format
      .read(s"""
                   |  {
                   |    "$key": ["$name"]
                   |  }
                   |  """.stripMargin.parseJson)
      .fields(key)
      .asInstanceOf[JsArray]
      .elements
      .head
      .asJsObject
      .fields(GROUP_KEY) shouldBe JsString(GROUP_DEFAULT)

    val values = format.read(s"""
                   |  {
                   |    "$key": [
                   |      {
                   |        "name": "$name"
                   |      },
                   |      "$name2"
                   |    ]
                   |  }
                   |  """.stripMargin.parseJson).fields(key).asInstanceOf[JsArray].elements
    values.size shouldBe 2
    values.map(_.asJsObject.fields).foreach(_(GROUP_KEY) shouldBe JsString(GROUP_DEFAULT))
    values.head.asJsObject.fields(NAME_KEY) shouldBe JsString(name)
    values.last.asJsObject.fields(NAME_KEY) shouldBe JsString(name2)
  }

  @Test
  def testDefineReadonlyField(): Unit = {
    val key   = CommonUtils.randomString()
    val value = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).optional(value).permission(SettingDef.Permission.READ_ONLY).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": "a"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsString(value)

    format.read(s"""
               |  {
               |  }
               |  """.stripMargin.parseJson).fields(key) shouldBe JsString(value)
  }

  @Test
  def testInternalSettingShouldBeRemoved(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).internal().build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": "a"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsString("a")
  }

  @Test
  def testCreateOnly(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).permission(Permission.CREATE_ONLY).build())
      .build

    format.read(s"""
                   |  {
                   |    "$key": "a"
                   |  }
                   |  """.stripMargin.parseJson).fields(key) shouldBe JsString("a")
  }

  @Test
  def testNameStringRestriction(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).regex(SettingDef.NAME_STRING_REGEX).build())
      .build

    // good case
    Set("ab", "a.", "123123").foreach(s => format.read(s"""
                                              |  {
                                              |    "$key": "$s"
                                              |  }
                                              |  """.stripMargin.parseJson))

    Set("B", "-", "_", "=", "~", CommonUtils.randomString(100))
      .foreach { s =>
        withClue(s"input:$s") {
          an[DeserializationException] should be thrownBy format.read(s"""
                                                                         |  {
                                                                         |    "$key": "$s"
                                                                         |  }
                                                                         |  """.stripMargin.parseJson)
        }
      }
  }

  @Test
  def testGroupStringRestriction(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).regex(SettingDef.GROUP_STRING_REGEX).build())
      .build

    // good case
    Set("ab", "a.", "123123").foreach(s => format.read(s"""
                                                          |  {
                                                          |    "$key": "$s"
                                                          |  }
                                                          |  """.stripMargin.parseJson))

    Set("B", "-", "_", "=", "~", CommonUtils.randomString(100))
      .foreach { s =>
        withClue(s"input:$s") {
          an[DeserializationException] should be thrownBy format.read(s"""
                                                                         |  {
                                                                         |    "$key": "$s"
                                                                         |  }
                                                                         |  """.stripMargin.parseJson)
        }
      }
  }

  @Test
  def testPrefix(): Unit = {
    val key    = CommonUtils.randomString()
    val prefix = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).stringWithRandomDefault(prefix).build())
      .build

    format.read(s"""
                   |  {
                   |  }
                   |  """.stripMargin.parseJson).fields(key).convertTo[String] should include(prefix)
  }

  @Test
  def testToBuilder(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .definition(SettingDef.builder().key(key).build())
      .build

    // pass
    format.read(s"""
                   |  {
                   |    "$key": "a"
                   |  }
                   |  """.stripMargin.parseJson)

    val key2 = CommonUtils.randomString()
    an[DeserializationException] should be thrownBy format.toBuilder
      .definitions(Seq(SettingDef.builder().key(key2).build()))
      .build()
      .read(s"""
               |  {
               |    "$key": "a"
               |  }
               |  """.stripMargin.parseJson)
  }

  @Test
  def testRemoveKey(): Unit = {
    val key = CommonUtils.randomString()
    val format = JsonRefinerBuilder[JsObject]
      .format(new RootJsonFormat[JsObject] {
        override def read(json: JsValue): JsObject = json.asJsObject

        override def write(obj: JsObject): JsValue = obj
      })
      .ignoreKeys(Set(key))
      .build

    format.read(s"""
                   |  {
                   |    "c": "aa",
                   |    "$key": "a"
                   |  }
                   |  """.stripMargin.parseJson).fields.get(key) shouldBe None
  }
}
