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

import java.io.File;
import java.net.InetSocketAddress;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public interface Zookeepers extends Releasable {

  /** @return zookeeper information. the form is "host_a:port_a,host_b:port_b" */
  String connectionProps();

  /** @return true if this zookeeper cluster is generated locally. */
  boolean isLocal();

  static Zookeepers local(int port) {
    final NIOServerCnxnFactory factory;
    File snapshotDir = CommonUtils.createTempFolder("local_zk_snapshot");
    File logDir = CommonUtils.createTempFolder("local_zk_log");

    try {
      // disable zookeeper.forceSync to avoid timeout
      System.setProperty("zookeeper.forceSync", "no");
      factory = new NIOServerCnxnFactory();
      factory.configure(
          new InetSocketAddress(CommonUtils.anyLocalAddress(), Math.max(0, port)), 1024);
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new Zookeepers() {
      @Override
      public void close() {
        factory.shutdown();
        CommonUtils.deleteFiles(snapshotDir);
        CommonUtils.deleteFiles(logDir);
      }

      @Override
      public String connectionProps() {
        return CommonUtils.hostname() + ":" + factory.getLocalPort();
      }

      @Override
      public boolean isLocal() {
        return true;
      }
    };
  }
}
