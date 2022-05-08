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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestProducerSender extends OharaTest {

  private static class FakeSender<K, V> extends Producer.Sender<K, V> {
    @Override
    public CompletableFuture<RecordMetadata> doSend() {
      return null;
    }
  }

  private static FakeSender<String, String> fake() {
    return new FakeSender<>();
  }

  @Test
  public void nullHeader() {
    Assertions.assertThrows(NullPointerException.class, () -> fake().header(null));
  }

  @Test
  public void nullHeaders() {
    Assertions.assertThrows(NullPointerException.class, () -> fake().headers(null));
  }

  @Test
  public void emptyHeaders() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> fake().headers(List.of()));
  }

  @Test
  public void nullKey() {
    Assertions.assertThrows(NullPointerException.class, () -> fake().key(null));
  }

  @Test
  public void nullValue() {
    Assertions.assertThrows(NullPointerException.class, () -> fake().value(null));
  }

  @Test
  public void nullTopicKey() {
    Assertions.assertThrows(NullPointerException.class, () -> fake().topicKey(null));
  }
}
