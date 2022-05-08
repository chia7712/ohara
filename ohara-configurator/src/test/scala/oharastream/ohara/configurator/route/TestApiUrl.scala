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

package oharastream.ohara.configurator.route

import oharastream.ohara.common.rule.OharaTest
import com.typesafe.scalalogging.Logger
import org.junit.jupiter.api.{Disabled, Test}
import org.scalatest.matchers.should.Matchers._

import sys.process._

class TestApiUrl extends OharaTest {
  private[this] val log = Logger(classOf[TestApiUrl])

  @Disabled("enable this test if the web site is available")
  @Test
  def testDocumentApiUrl(): Unit = {
    val command = "curl --silent --url %s".format(oharastream.ohara.configurator.route.apiUrl)
    log.info("fetch html: {}", command)

    (command.!!) should include("""<h1>Ohara REST Interface</h1>""")
  }
}
