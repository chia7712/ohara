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

package oharastream.ohara.testing;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.testing.service.FtpServer;
import oharastream.ohara.testing.service.Hdfs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOharaTestUtils extends OharaTest {

  @Test
  public void getBrokerFromEmptyOharaTestUtil() {
    try (OharaTestUtils util = OharaTestUtils.of()) {
      String connectionProps = null;
      for (int i = 0; i != 10; ++i) {
        if (connectionProps == null) connectionProps = util.brokersConnProps();
        else Assertions.assertEquals(connectionProps, util.brokersConnProps());
      }
    }
  }

  @Test
  public void getWorkerFromEmptyOharaTestUtil() {
    try (OharaTestUtils util = OharaTestUtils.of()) {
      String connectionProps = null;
      for (int i = 0; i != 10; ++i) {
        if (connectionProps == null) connectionProps = util.workersConnProps();
        else Assertions.assertEquals(connectionProps, util.workersConnProps());
      }
    }
  }

  @Test
  public void testFtpServer() {
    try (OharaTestUtils util = OharaTestUtils.of()) {
      FtpServer fs = null;
      for (int i = 0; i != 10; ++i) {
        if (fs == null) fs = util.ftpServer();
        else Assertions.assertEquals(fs, util.ftpServer());
      }
    }
  }

  @Test
  public void testHdfs() {
    try (OharaTestUtils util = OharaTestUtils.of()) {
      Hdfs hdfs = null;
      for (int i = 0; i != 10; ++i) {
        if (hdfs == null) hdfs = util.hdfs();
        else Assertions.assertEquals(hdfs, util.hdfs());
      }
    }
  }
}
