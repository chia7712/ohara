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

import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.testing.With3Brokers;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTopicAdmin extends With3Brokers {
  private final TopicAdmin client = TopicAdmin.of(testUtil().brokersConnProps());
  private final TopicKey topicKey = TopicKey.of("TestTopicAdmin", CommonUtils.randomString(5));

  private void waitPartitions(TopicKey topicKey, int numberOfPartitions) {
    CommonUtils.await(
        () -> {
          try {
            return client
                    .topicDescription(topicKey)
                    .toCompletableFuture()
                    .get()
                    .numberOfPartitions()
                == numberOfPartitions;
          } catch (Exception e) {
            return false;
          }
        },
        Duration.ofSeconds(30));
  }

  @Test
  public void testAddPartitions() throws ExecutionException, InterruptedException {
    int numberOfPartitions = 1;
    short numberOfReplications = 1;
    client
        .topicCreator()
        .numberOfPartitions(numberOfPartitions)
        .numberOfReplications(numberOfReplications)
        .topicKey(topicKey)
        .create()
        .toCompletableFuture()
        .get();
    waitPartitions(topicKey, numberOfPartitions);
    Assertions.assertEquals(
        numberOfPartitions,
        client.topicDescription(topicKey).toCompletableFuture().get().numberOfPartitions());

    numberOfPartitions = 2;
    client.createPartitions(topicKey, numberOfPartitions).toCompletableFuture().get();
    waitPartitions(topicKey, numberOfPartitions);
    Assertions.assertEquals(
        numberOfPartitions,
        client.topicDescription(topicKey).toCompletableFuture().get().numberOfPartitions());
    // decrease the number
    Assertions.assertThrows(
        Exception.class, () -> client.createPartitions(topicKey, 1).toCompletableFuture().get());
    // alter an nonexistent topic
    Assertions.assertThrows(
        NoSuchElementException.class,
        () -> {
          try {
            client
                .createPartitions(TopicKey.of("a", CommonUtils.randomString(5)), 2)
                .toCompletableFuture()
                .get();
          } catch (ExecutionException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  public void testCreate() throws ExecutionException, InterruptedException {
    int numberOfPartitions = 2;
    short numberOfReplications = (short) 2;
    client
        .topicCreator()
        .numberOfPartitions(numberOfPartitions)
        .numberOfReplications(numberOfReplications)
        .topicKey(topicKey)
        .create()
        .toCompletableFuture()
        .get();
    waitPartitions(topicKey, numberOfPartitions);
    TopicDescription topicInfo = client.topicDescription(topicKey).toCompletableFuture().get();

    Assertions.assertEquals(topicKey, topicInfo.topicKey());

    Assertions.assertEquals(numberOfPartitions, topicInfo.numberOfPartitions());
    Assertions.assertEquals(numberOfReplications, topicInfo.numberOfReplications());

    Assertions.assertEquals(
        topicInfo, client.topicDescription(topicKey).toCompletableFuture().get());

    client.deleteTopic(topicKey).toCompletableFuture().get();
    Assertions.assertFalse(client.exist(topicKey).toCompletableFuture().get());
  }

  @Test
  public void testTopicOptions() throws ExecutionException, InterruptedException {
    int numberOfPartitions = 2;
    short numberOfReplications = (short) 2;
    Map<String, String> options =
        Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    client
        .topicCreator()
        .numberOfPartitions(numberOfPartitions)
        .numberOfReplications(numberOfReplications)
        .options(options)
        .topicKey(topicKey)
        .create()
        .toCompletableFuture()
        .get();
    waitPartitions(topicKey, numberOfPartitions);

    TopicDescription topicInfo = client.topicDescription(topicKey).toCompletableFuture().get();

    Assertions.assertEquals(topicKey, topicInfo.topicKey());

    Assertions.assertEquals(numberOfPartitions, topicInfo.numberOfPartitions());
    Assertions.assertEquals(numberOfReplications, topicInfo.numberOfReplications());

    Assertions.assertEquals(
        TopicConfig.CLEANUP_POLICY_DELETE,
        topicInfo.options().stream()
            .filter(x -> Objects.equals(x.key(), TopicConfig.CLEANUP_POLICY_CONFIG))
            .collect(Collectors.toUnmodifiableList())
            .get(0)
            .value());
  }

  @AfterEach
  public void cleanup() throws ExecutionException, InterruptedException {
    client
        .topicKeys()
        .toCompletableFuture()
        .get()
        .forEach(
            key -> {
              try {
                client.deleteTopic(key).toCompletableFuture().get();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    Releasable.close(client);
  }
}
