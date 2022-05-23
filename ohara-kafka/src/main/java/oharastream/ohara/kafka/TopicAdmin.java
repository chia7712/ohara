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

package oharastream.ohara.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * a helper methods used by configurator. It provide many helper method to operate kafka cluster.
 * TODO: Do we really need a jave version of "broker" APIs? I don't think it is ok to allow user to
 * touch topic directly... by chia
 *
 * <p>Every method with remote call need to overload in Default Timeout
 */
public interface TopicAdmin extends Releasable {

  /**
   * start a progress to create topic
   *
   * @return creator
   */
  TopicCreator topicCreator();

  /**
   * @param key topic key
   * @return true if topic exists. Otherwise, false
   */
  CompletionStage<Boolean> exist(TopicKey key);

  /**
   * list all ohara topic keys. Noted that non-ohara topics are excluded
   *
   * @return ohara topic keys
   */
  CompletionStage<Set<TopicKey>> topicKeys();

  /**
   * describe the topic existing in kafka. If the topic doesn't exist, exception will be thrown
   *
   * @param key topic key
   * @return TopicDescription
   */
  CompletionStage<TopicDescription> topicDescription(TopicKey key);

  /**
   * create new partitions for specified topic
   *
   * @param key topic key
   * @param numberOfPartitions number of new partitions
   * @return a async callback is tracing the result
   */
  CompletionStage<Void> createPartitions(TopicKey key, int numberOfPartitions);

  /**
   * remove topic.
   *
   * @param key topic key
   * @return a async callback is tracing the result. It carries the "true" if it does remove a
   *     topic. otherwise, false
   */
  CompletionStage<Boolean> deleteTopic(TopicKey key);

  /** @return Connection information. form: host:port,host:port */
  String connectionProps();

  /**
   * list all active brokers' ports. This is different to {@link #connectionProps()} since this
   * method will fetch the "really" active brokers from cluster. {@link #connectionProps()} may
   * include dead or nonexistent broker nodes.
   *
   * @return active brokers' ports
   */
  CompletionStage<Map<String, Integer>> brokerPorts();

  boolean closed();

  static TopicAdmin of(String connectionProps) {
    return new TopicAdmin() {

      private final AdminClient admin = AdminClient.create(toAdminProps(connectionProps));

      private final AtomicBoolean closed = new AtomicBoolean(false);

      private Map<TopicKey, List<PartitionInfo>> partitionInfos(Set<TopicKey> topicKeys) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionProps);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CommonUtils.randomString());
        try (KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<>(
                properties, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
          return consumer.listTopics().entrySet().stream()
              .filter(
                  entry -> TopicKey.ofPlain(entry.getKey()).map(topicKeys::contains).isPresent())
              .map(
                  entry -> {
                    List<TopicPartition> tps =
                        entry.getValue().stream()
                            .map(p -> new TopicPartition(p.topic(), p.partition()))
                            .collect(Collectors.toUnmodifiableList());
                    Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tps);
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
                    List<PartitionInfo> ps =
                        entry.getValue().stream()
                            .map(
                                p -> {
                                  TopicPartition tp = new TopicPartition(p.topic(), p.partition());
                                  return new PartitionInfo(
                                      p.partition(),
                                      PartitionNode.of(p.leader()),
                                      p.replicas() == null
                                          ? List.of()
                                          : Arrays.stream(p.replicas())
                                              .map(PartitionNode::of)
                                              .collect(Collectors.toUnmodifiableList()),
                                      p.inSyncReplicas() == null
                                          ? List.of()
                                          : Arrays.stream(p.inSyncReplicas())
                                              .map(PartitionNode::of)
                                              .collect(Collectors.toUnmodifiableList()),
                                      beginningOffsets.getOrDefault(tp, -1L),
                                      endOffsets.getOrDefault(tp, -1L));
                                })
                            .collect(Collectors.toUnmodifiableList());
                    return Map.entry(TopicKey.requirePlain(entry.getKey()), ps);
                  })
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
      }

      @Override
      public TopicCreator topicCreator() {
        return new TopicCreator() {
          @Override
          protected CompletionStage<Void> doCreate(
              int numberOfPartitions,
              short numberOfReplications,
              Map<String, String> options,
              TopicKey topicKey) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            admin
                .createTopics(
                    List.of(
                        new NewTopic(
                                topicKey.topicNameOnKafka(),
                                numberOfPartitions,
                                numberOfReplications)
                            .configs(options)))
                .values()
                .get(topicKey.topicNameOnKafka())
                .whenComplete(
                    (v, exception) -> {
                      if (exception != null) f.completeExceptionally(exception);
                      else f.complete(null);
                    });
            return f;
          }
        };
      }

      @Override
      public CompletionStage<Boolean> exist(TopicKey key) {
        return topicKeys().thenApply(keys -> keys.contains(key));
      }

      @Override
      public CompletionStage<Set<TopicKey>> topicKeys() {
        CompletableFuture<Set<TopicKey>> f = new CompletableFuture<>();
        admin
            .listTopics()
            .names()
            .whenComplete(
                (tps, exception) -> {
                  if (exception != null) f.completeExceptionally(exception);
                  else
                    f.complete(
                        tps.stream()
                            .map(TopicKey::ofPlain)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.toUnmodifiableSet()));
                });
        return f;
      }

      private CompletionStage<Map<TopicKey, List<TopicOption>>> options(Set<TopicKey> topicKeys) {
        return topicKeys()
            .thenApply(
                keys ->
                    keys.stream()
                        .filter(topicKeys::contains)
                        .map(
                            key ->
                                new ConfigResource(
                                    ConfigResource.Type.TOPIC, key.topicNameOnKafka()))
                        .collect(Collectors.toUnmodifiableList()))
            .thenCompose(
                rs -> {
                  CompletableFuture<Map<TopicKey, List<TopicOption>>> f = new CompletableFuture<>();
                  admin
                      .describeConfigs(rs)
                      .all()
                      .whenComplete(
                          (configs, exception) -> {
                            if (exception != null) f.completeExceptionally(exception);
                            else
                              f.complete(
                                  configs.entrySet().stream()
                                      .map(
                                          entry ->
                                              Map.entry(
                                                  TopicKey.requirePlain(entry.getKey().name()),
                                                  entry.getValue().entries().stream()
                                                      .map(
                                                          o ->
                                                              new TopicOption(
                                                                  o.name(),
                                                                  o.value(),
                                                                  o.isDefault(),
                                                                  o.isSensitive(),
                                                                  o.isReadOnly()))
                                                      .collect(Collectors.toUnmodifiableList())))
                                      .collect(
                                          Collectors.toUnmodifiableMap(
                                              Map.Entry::getKey, Map.Entry::getValue)));
                          });
                  return f;
                });
      }

      @Override
      public CompletionStage<TopicDescription> topicDescription(TopicKey key) {
        return topicDescriptions(Set.of(key)).thenApply(keys -> keys.iterator().next());
      }

      private CompletionStage<List<TopicDescription>> topicDescriptions(Set<TopicKey> topicKeys) {
        return options(topicKeys)
            .thenApply(
                nameAndOpts -> {
                  Map<TopicKey, List<PartitionInfo>> partitionInfos = partitionInfos(topicKeys);
                  return nameAndOpts.entrySet().stream()
                      .map(
                          entry -> {
                            List<PartitionInfo> infos =
                                partitionInfos.getOrDefault(entry.getKey(), List.of());
                            return new TopicDescription(entry.getKey(), infos, entry.getValue());
                          })
                      .collect(Collectors.toUnmodifiableList());
                });
      }

      @Override
      public CompletionStage<Void> createPartitions(TopicKey key, int numberOfPartitions) {
        return topicDescription(key)
            .thenCompose(
                current -> {
                  if (current.numberOfPartitions() > numberOfPartitions)
                    throw new IllegalArgumentException(
                        "Reducing the number from partitions is disallowed. current:"
                            + current.numberOfPartitions()
                            + ", expected:"
                            + numberOfPartitions);
                  else if (current.numberOfPartitions() == numberOfPartitions)
                    return CompletableFuture.completedFuture(null);
                  else {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    admin
                        .createPartitions(
                            Map.of(
                                key.topicNameOnKafka(),
                                NewPartitions.increaseTo(numberOfPartitions)))
                        .values()
                        .get(key.topicNameOnKafka())
                        .whenComplete(
                            (v, exception) -> {
                              if (exception != null) f.completeExceptionally(exception);
                              else f.complete(null);
                            });
                    return f;
                  }
                });
      }

      @Override
      public CompletionStage<Boolean> deleteTopic(TopicKey key) {
        return exist(key)
            .thenCompose(
                existent -> {
                  if (existent) {
                    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
                    admin
                        .deleteTopics(List.of(key.topicNameOnKafka()))
                        .topicNameValues()
                        .get(key.topicNameOnKafka())
                        .whenComplete(
                            (v, exception) -> {
                              if (exception != null) f2.completeExceptionally(exception);
                              else f2.complete(true);
                            });
                    return f2;
                  } else return CompletableFuture.completedFuture(false);
                });
      }

      @Override
      public String connectionProps() {
        return connectionProps;
      }

      @Override
      public CompletionStage<Map<String, Integer>> brokerPorts() {
        CompletableFuture<Map<String, Integer>> f = new CompletableFuture<>();
        admin
            .describeCluster()
            .nodes()
            .whenComplete(
                (nodes, exception) -> {
                  if (exception != null) f.completeExceptionally(exception);
                  else
                    f.complete(
                        nodes.stream()
                            .collect(Collectors.toUnmodifiableMap(Node::host, Node::port)));
                });
        return f;
      }

      /**
       * this impl will host a kafka.AdminClient so you must call the #close() to release the
       * kafka.AdminClient.
       *
       * @param brokers the kafka brokers information
       * @return the props for TopicAdmin
       */
      private Properties toAdminProps(String brokers) {
        Properties adminProps = new Properties();
        adminProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return adminProps;
      }

      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          admin.close();
        }
      }

      @Override
      public boolean closed() {
        return closed.get();
      }
    };
  }
}
