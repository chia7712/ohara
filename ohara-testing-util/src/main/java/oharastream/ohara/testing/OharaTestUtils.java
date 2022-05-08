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

import java.util.Arrays;
import java.util.stream.IntStream;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.common.util.ReleaseOnce;
import oharastream.ohara.testing.service.*;

/**
 * This class create a kafka services having 1 zk instance and 1 broker default. Also, this class
 * have many helper methods to make test more friendly.
 *
 * <p>{@code How to use this class: 1) create the OharaTestUtils with 1 broker (you can assign
 * arbitrary number from brokers) val testUtil = OharaTestUtils.localBrokers(1) 2) get the
 * basic|producer|consumer OharaConfiguration val setting = testUtil.producerConfig 3) instantiate
 * your producer or consumer val producer = new KafkaProducer<Array<Byte>, Array<Byte>>(setting, new
 * ByteArraySerializer, new ByteArraySerializer) 4) do what you want for your producer and consumer
 * 5) close OharaTestUtils testUtil.close() }
 *
 * <p>see TestOharaTestUtil for more examples NOTED: the close() will shutdown all services
 * including the passed consumers (see run())
 */
public class OharaTestUtils extends ReleaseOnce {
  private FtpServer ftpServer;
  private Hdfs hdfs;
  private Zookeepers zk;
  private Brokers brokers;
  private Workers workers;

  private OharaTestUtils() {}

  /**
   * Exposing the brokers connection. This list should be in the form <code>
   * host1:port1,host2:port2,...</code>.
   *
   * @return brokers connection information
   */
  public String brokersConnProps() {
    try2CreateBrokers(1);
    return brokers.connectionProps();
  }

  private void try2CreateZookeeper() {
    if (zk == null) {
      zk = Zookeepers.local(0);
    }
  }

  private void try2CreateBrokers(int numberOfBrokers) {
    if (brokers == null) {
      try2CreateZookeeper();
      brokers = Brokers.local(zk, IntStream.range(0, numberOfBrokers).map(i -> 0).toArray());
    }
  }

  private void try2CreateWorkers(int numberOfWorkers) {
    if (workers == null) {
      try2CreateBrokers(1);
      workers = Workers.local(brokers, IntStream.range(0, numberOfWorkers).map(i -> 0).toArray());
    }
  }

  /**
   * Exposing the workers connection. This list should be in the form <code>
   * host1:port1,host2:port2,...</code>.
   *
   * @return workers connection information
   */
  public String workersConnProps() {
    try2CreateWorkers(1);
    return workers.connectionProps();
  }

  public Hdfs hdfs() {
    if (hdfs == null) hdfs = Hdfs.local();
    return hdfs;
  }

  public FtpServer ftpServer() {
    if (ftpServer == null)
      ftpServer =
          FtpServer.builder()
              // 3 data ports -> 3 connection
              .dataPorts(Arrays.asList(0, 0, 0))
              .build();
    return ftpServer;
  }

  @Override
  protected void doClose() {
    Releasable.close(ftpServer);
    Releasable.close(hdfs);
    Releasable.close(workers);
    Releasable.close(brokers);
    Releasable.close(zk);
  }

  /**
   * create a test util with a broker cluster based on single node. NOTED: don't call the worker and
   * hdfs service. otherwise you will get exception
   *
   * @return a test util
   */
  static OharaTestUtils broker() {
    return brokers(1);
  }

  /**
   * Create a test util with multi-brokers. NOTED: don't call the worker and hdfs service. otherwise
   * you will get exception
   *
   * @return a test util
   */
  static OharaTestUtils brokers(int numberOfBrokers) {
    OharaTestUtils util = new OharaTestUtils();
    util.try2CreateBrokers(numberOfBrokers);
    return util;
  }

  /**
   * create a test util with a worker/broker cluster based on single node. NOTED: don't call the
   * worker and hdfs service. otherwise you will get exception
   *
   * @return a test util
   */
  static OharaTestUtils worker() {
    return workers(1, 1);
  }

  /**
   * Create a test util with multi-brokers and multi-workers. NOTED: don't call the hdfs service.
   * otherwise you will get exception.
   *
   * <p>NOTED: the default number of brokers is 3
   *
   * @return a test util
   */
  static OharaTestUtils workers(int numberOfBrokers, int numberOfWorkers) {
    OharaTestUtils util = new OharaTestUtils();
    util.try2CreateBrokers(numberOfBrokers);
    util.try2CreateWorkers(numberOfWorkers);
    return util;
  }

  /**
   * Create a test util without any pre-created services.
   *
   * @return a test util
   */
  static OharaTestUtils of() {
    return new OharaTestUtils();
  }
}
