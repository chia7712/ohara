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

import java.io.IOException;
import java.net.BindException;
import java.util.*;
import java.util.stream.Collectors;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;

public interface Workers extends Releasable {
  /** @return workers information. the form is "host_a:port_a,host_b:port_b" */
  String connectionProps();

  /** @return true if this worker cluster is generated locally. */
  boolean isLocal();

  static Workers local(Brokers brokers, int[] ports) {
    List<Integer> availablePorts = new ArrayList<>(ports.length);
    List<Connect> connects =
        Arrays.stream(ports)
            .mapToObj(
                port -> {
                  boolean canRetry = port <= 0;
                  while (true) {
                    try {
                      int availablePort = CommonUtils.resolvePort(port);

                      Map<String, String> config = new HashMap<>();
                      // reduce the number from partitions and replicas to speedup the mini cluster
                      // for setting storage. the partition from setting topic is always 1 so we
                      // needn't to set it to 1 here.
                      config.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-config");
                      config.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
                      // for offset storage
                      config.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
                      config.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, "1");
                      config.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
                      // for status storage
                      config.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
                      config.put(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG, "1");
                      config.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
                      // set the brokers info
                      config.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.connectionProps());
                      config.put(ConsumerConfig.GROUP_ID_CONFIG, "connect");
                      // set the normal converter
                      config.put(
                          ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
                          "org.apache.kafka.connect.json.JsonConverter");
                      config.put("key.converter.schemas.enable", "true");
                      config.put(
                          ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
                          "org.apache.kafka.connect.json.JsonConverter");
                      config.put("value.converter.schemas.enable", "true");
                      config.put(
                          WorkerConfig.LISTENERS_CONFIG,
                          // the worker hostname is a part of information used by restful apis.
                          // the 0.0.0.0 make all connector say that they are executed by 0.0.0.0
                          // and it does make sense in production. With a view to testing the
                          // related codes in other modules, we have to define the "really" hostname
                          // in starting worker cluster.
                          "http://" + CommonUtils.hostname() + ":" + availablePort);
                      config.put(
                          WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(500));
                      // enable us to override the connector configs
                      config.put(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");

                      Connect connect = new ConnectDistributed().startConnect(config);
                      availablePorts.add(availablePort);
                      return connect;
                    } catch (ConnectException e) {
                      if (!canRetry
                          // the binding error may be wrapped to IOException so we have to check the
                          // error message.
                          || !(e.getCause() instanceof IOException
                              && e.getMessage().contains("Failed to bind"))
                          || !(e.getCause() instanceof BindException)) throw e;
                    }
                  }
                })
            .collect(Collectors.toUnmodifiableList());
    return new Workers() {
      @Override
      public void close() {
        connects.forEach(Connect::stop);
        connects.forEach(Connect::awaitStop);
      }

      @Override
      public String connectionProps() {
        return availablePorts.stream()
            .map(p -> CommonUtils.hostname() + ":" + p)
            .collect(Collectors.joining(","));
      }

      @Override
      public boolean isLocal() {
        return true;
      }
    };
  }
}
