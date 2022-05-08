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

package oharastream.ohara.testing.service;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSshdServer extends OharaTest {

  @Test
  public void testSpecificPort() {
    int port = CommonUtils.availablePort();
    try (SshdServer server = SshdServer.local(port)) {
      Assertions.assertEquals(server.port(), port);
    }
  }

  @Test
  public void testRandomPort() {
    try (SshdServer server = SshdServer.local(0)) {
      Assertions.assertNotEquals(server.port(), 0);
    }
  }
}
