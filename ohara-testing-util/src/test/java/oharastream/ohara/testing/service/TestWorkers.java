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

public class TestWorkers extends OharaTest {

  @Test
  public void testSpecificPort() {
    int[] brokerPorts = {0};
    int workerPort = CommonUtils.availablePort();

    try (Zookeepers zk = Zookeepers.local(0);
        Brokers brokers = Brokers.local(zk, brokerPorts);
        Workers workers = Workers.local(brokers, new int[] {workerPort})) {
      Assertions.assertEquals(
          workerPort, Integer.parseInt(workers.connectionProps().split(",")[0].split(":")[1]));
    }
  }

  @Test
  public void testRandomPort() throws Exception {
    int[] brokerPorts = {0};
    int[] workerPorts = {0};

    try (Zookeepers zk = Zookeepers.local(0);
        Brokers brokers = Brokers.local(zk, brokerPorts);
        Workers workers = Workers.local(brokers, workerPorts)) {
      Assertions.assertNotEquals(
          0, Integer.parseInt(workers.connectionProps().split(",")[0].split(":")[1]));
    }
  }
}
