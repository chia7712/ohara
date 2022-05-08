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

package oharastream.ohara.stream.ostream;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOStreamBuilder extends OharaTest {

  private final ObjectKey key = CommonUtils.randomKey();

  @Test
  public void nullBootstrapServers() {
    Assertions.assertThrows(
        NullPointerException.class, () -> OStreamBuilder.builder().bootstrapServers(null));
  }

  @Test
  public void emptyBootstrapServers() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> OStreamBuilder.builder().bootstrapServers(""));
  }

  @Test
  public void nullKey() {
    Assertions.assertThrows(NullPointerException.class, () -> OStreamBuilder.builder().key(null));
  }

  @Test
  public void nullFromTopic() {
    Assertions.assertThrows(
        NullPointerException.class, () -> OStreamBuilder.builder().fromTopic(null));
  }

  @Test
  public void emptyFromTopic() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> OStreamBuilder.builder().fromTopic(""));
  }

  @Test
  public void nullToTopic() {
    Assertions.assertThrows(
        NullPointerException.class, () -> OStreamBuilder.builder().toTopic(null));
  }

  @Test
  public void emptyToTopic() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> OStreamBuilder.builder().toTopic(""));
  }

  @Test
  public void minimumBuilder() {
    OStreamBuilder.builder()
        .key(key)
        .bootstrapServers(CommonUtils.randomString())
        .fromTopic(CommonUtils.randomString())
        .toTopic(CommonUtils.randomString())
        .build();
  }
}
