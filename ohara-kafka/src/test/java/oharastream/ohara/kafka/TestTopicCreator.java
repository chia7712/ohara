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

import java.util.Map;
import java.util.concurrent.CompletionStage;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTopicCreator extends OharaTest {

  private static class FakeTopicCreator extends TopicCreator {
    @Override
    protected CompletionStage<Void> doCreate(
        int numberOfPartitions,
        short numberOfReplications,
        Map<String, String> options,
        TopicKey topicKey) {
      return null;
    }
  }

  private static TopicCreator fake() {
    return new FakeTopicCreator();
  }

  @Test
  public void illegalNumberOfPartitions() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> fake().numberOfPartitions(-1));
  }

  @Test
  public void illegalNumberOfReplications() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> fake().numberOfReplications((short) -1));
  }

  @Test
  public void nullOptions() {
    Assertions.assertThrows(NullPointerException.class, () -> fake().options(null));
  }

  @Test
  public void emptyOptions() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> fake().options(Map.of()));
  }
}
