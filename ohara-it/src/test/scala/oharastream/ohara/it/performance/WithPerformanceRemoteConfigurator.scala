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

package oharastream.ohara.it.performance

import oharastream.ohara.common.util.Releasable
import oharastream.ohara.it.{ContainerPlatform, IntegrationTest}
import org.junit.jupiter.api.{AfterEach, Tag}

/**
  * a basic setup offering a configurator running on remote node.
  * this stuff is also in charge of releasing the configurator after testing.
  */
@Tag("performance")
private[performance] abstract class WithPerformanceRemoteConfigurator extends IntegrationTest {
  private[this] val platform                               = ContainerPlatform.default
  protected val resourceRef: ContainerPlatform.ResourceRef = platform.setup()
  protected val configuratorHostname: String               = resourceRef.configuratorHostname
  protected val configuratorPort: Int                      = resourceRef.configuratorPort
  @AfterEach
  def releaseConfigurator(): Unit = Releasable.close(resourceRef)
}
