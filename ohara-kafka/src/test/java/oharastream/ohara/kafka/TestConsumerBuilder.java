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

import java.util.Set;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.kafka.connector.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConsumerBuilder extends OharaTest {

  @Test
  public void nullGroupId() {
    Assertions.assertThrows(NullPointerException.class, () -> Consumer.builder().groupId(null));
  }

  @Test
  public void nullTopicKey() {
    Assertions.assertThrows(NullPointerException.class, () -> Consumer.builder().topicKey(null));
  }

  @Test
  public void nullTopicKeys() {
    Assertions.assertThrows(NullPointerException.class, () -> Consumer.builder().topicKeys(null));
  }

  @Test
  public void emptyTopicKeys() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Consumer.builder().topicKeys(Set.of()));
  }

  @Test
  public void nullConnectionProps() {
    Assertions.assertThrows(
        NullPointerException.class, () -> Consumer.builder().connectionProps(null));
  }

  @Test
  public void emptyConnectionProps() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Consumer.builder().connectionProps(""));
  }

  @Test
  public void nullKeySerializer() {
    Assertions.assertThrows(
        NullPointerException.class, () -> Consumer.builder().keySerializer(null));
  }

  @Test
  public void nullValueSerializer() {
    Assertions.assertThrows(
        NullPointerException.class, () -> Consumer.builder().valueSerializer(null));
  }

  @Test
  public void emptyAssignments() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Consumer.builder().assignments(Set.of()));
  }

  @Test
  public void nullAssignments() {
    Assertions.assertThrows(NullPointerException.class, () -> Consumer.builder().assignments(null));
  }

  @Test
  public void assignBothTopicAndAssignments() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Consumer.builder()
                .assignments(Set.of(new TopicPartition(TopicKey.of("g", "n"), 1)))
                .topicKey(TopicKey.of("a", "b")));
  }
}
