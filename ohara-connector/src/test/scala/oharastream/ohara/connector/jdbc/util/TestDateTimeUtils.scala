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

package oharastream.ohara.connector.jdbc.util

import java.sql.Timestamp

import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestDateTimeUtils extends OharaTest {
  @Test
  def testTaipeiTimeZone(): Unit = {
    val timestamp: Timestamp = new Timestamp(0)
    timestamp.toString shouldBe "1970-01-01 08:00:00.0"
  }
}
