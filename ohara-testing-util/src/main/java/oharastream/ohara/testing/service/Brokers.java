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
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.SystemTime;

public interface Brokers extends Releasable {
  /** @return brokers information. the form is "host_a:port_a,host_b:port_b" */
  String connectionProps();

  /** @return true if this broker cluster is generated locally. */
  boolean isLocal();

  static Brokers local(Zookeepers zk, int[] ports) {
    List<File> tempFolders =
        IntStream.range(0, ports.length)
            .mapToObj(i -> CommonUtils.createTempFolder("local_kafka"))
            .collect(Collectors.toUnmodifiableList());
    List<KafkaServer> brokers =
        IntStream.range(0, ports.length)
            .mapToObj(
                index -> {
                  int port = ports[index];
                  if (port > 65535)
                    throw new RuntimeException("port: " + port + " can't be larger than 65535");
                  File logDir = tempFolders.get(index);
                  Properties config = new Properties();
                  // reduce the number from partitions and replicas to speedup the mini cluster
                  config.setProperty(
                      KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), String.valueOf(1));
                  config.setProperty(
                      KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), String.valueOf(1));
                  config.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zk.connectionProps());
                  config.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), String.valueOf(index));
                  config.setProperty(
                      KafkaConfig$.MODULE$.ListenersProp(), "PLAINTEXT://:" + (Math.max(port, 0)));
                  config.setProperty(KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());
                  // increase the timeout in order to avoid ZkTimeoutException
                  config.setProperty(
                      KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), String.valueOf(30 * 1000));
                  scala.jdk.CollectionConverters.IterableHasAsScala(List.of());
                  KafkaServer broker =
                      new KafkaServer(
                          new KafkaConfig(config), SystemTime.SYSTEM, scala.Option.empty(), false);

                  broker.startup();
                  return broker;
                })
            .collect(Collectors.toUnmodifiableList());
    String connectionProps =
        brokers.stream()
            .map(
                broker ->
                    CommonUtils.hostname() + ":" + broker.boundPort(new ListenerName("PLAINTEXT")))
            .collect(Collectors.joining(","));
    return new Brokers() {

      @Override
      public void close() {
        brokers.forEach(
            broker -> {
              broker.shutdown();
              broker.awaitShutdown();
            });
        tempFolders.forEach(CommonUtils::deleteFiles);
      }

      @Override
      public String connectionProps() {
        return connectionProps;
      }

      @Override
      public boolean isLocal() {
        return true;
      }
    };
  }
}
