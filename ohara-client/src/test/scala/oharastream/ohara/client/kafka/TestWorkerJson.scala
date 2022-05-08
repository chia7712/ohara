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

package oharastream.ohara.client.kafka

import oharastream.ohara.client.kafka.WorkerJson._
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.{Creation, Validation}
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json._
class TestWorkerJson extends OharaTest {
  @Test
  def testValidatedValue(): Unit = {
    val validatedValue = KafkaValidatedValue(
      name = CommonUtils.randomString(5),
      value = Some("String"),
      errors = Seq(CommonUtils.randomString(5), CommonUtils.randomString(5))
    )
    KAFKA_VALIDATED_VALUE_FORMAT.read(KAFKA_VALIDATED_VALUE_FORMAT.write(validatedValue)) shouldBe validatedValue
  }

  @Test
  def testValidatedValueFromString(): Unit = {
    val name           = CommonUtils.randomString(5)
    val value          = CommonUtils.randomString(5)
    val error          = CommonUtils.randomString(5)
    val validatedValue = KAFKA_VALIDATED_VALUE_FORMAT.read(s"""
                                               |{
                                               |  "name":"$name",
                                               |  "value":"$value",
                                               |  "errors":["$error", "$error"]
                                               |}
                                            """.stripMargin.parseJson)
    validatedValue.name shouldBe name
    validatedValue.value shouldBe Some(value)
    validatedValue.errors shouldBe Seq(error, error)
  }

  @Test
  def testValidatedValueFromStringWithoutValue(): Unit = {
    val name           = CommonUtils.randomString(5)
    val error          = CommonUtils.randomString(5)
    val validatedValue = KAFKA_VALIDATED_VALUE_FORMAT.read(s"""
                                                        |{
                                                        |  "name":"$name",
                                                        |  "errors":["$error", "$error"]
                                                        |}
                                            """.stripMargin.parseJson)
    validatedValue.name shouldBe name
    validatedValue.value shouldBe None
    validatedValue.errors shouldBe Seq(error, error)
  }

  @Test
  def testValidatedValueFromStringWithEmptyValue(): Unit = {
    val name           = CommonUtils.randomString(5)
    val error          = CommonUtils.randomString(5)
    val validatedValue = KAFKA_VALIDATED_VALUE_FORMAT.read(s"""
                                                        |{
                                                        |  "name":"$name",
                                                        |  "value":"",
                                                        |  "errors":["$error", "$error"]
                                                        |}
                                            """.stripMargin.parseJson)
    validatedValue.name shouldBe name
    validatedValue.value shouldBe None
    validatedValue.errors shouldBe Seq(error, error)
  }

  @Test
  def testValidatedValueFromStringWithNullValue(): Unit = {
    val name           = CommonUtils.randomString(5)
    val error          = CommonUtils.randomString(5)
    val validatedValue = KAFKA_VALIDATED_VALUE_FORMAT.read(s"""
                                                        |{
                                                        |  "name":"$name",
                                                        |  "value":null,
                                                        |  "errors":["$error", "$error"]
                                                        |}
                                            """.stripMargin.parseJson)
    validatedValue.name shouldBe name
    validatedValue.value shouldBe None
    validatedValue.errors shouldBe Seq(error, error)
  }

  @Test
  def testCreation(): Unit = {
    val creation = Creation.of(CommonUtils.randomString(), CommonUtils.randomString(), CommonUtils.randomString())
    creation shouldBe CREATION_FORMAT.read(CREATION_FORMAT.write(creation))
  }

  @Test
  def testValidation(): Unit = {
    val validation = Validation.of(java.util.Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
    validation shouldBe KAFKA_VALIDATION_FORMAT.read(KAFKA_VALIDATION_FORMAT.write(validation))
  }
}
