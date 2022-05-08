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

package oharastream.ohara.metrics.basic;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCounterBuilder extends OharaTest {

  @Test
  public void testNullId() {
    Assertions.assertThrows(NullPointerException.class, () -> Counter.builder().id(null));
  }

  @Test
  public void testEmptyId() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Counter.builder().id(""));
  }

  @Test
  public void testNullGroup() {
    Assertions.assertThrows(NullPointerException.class, () -> Counter.builder().key(null));
  }

  @Test
  public void testNullName() {
    Assertions.assertThrows(NullPointerException.class, () -> Counter.builder().item(null));
  }

  @Test
  public void testEmptyName() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Counter.builder().item(""));
  }

  @Test
  public void testNullUnit() {
    Assertions.assertThrows(NullPointerException.class, () -> Counter.builder().unit(null));
  }

  @Test
  public void testEmptyUnit() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Counter.builder().unit(""));
  }

  @Test
  public void testNullDocument() {
    Assertions.assertThrows(NullPointerException.class, () -> Counter.builder().document(null));
  }

  @Test
  public void testEmptyDocument() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Counter.builder().document(""));
  }

  @Test
  public void testSetters() {
    ObjectKey key = CommonUtils.randomKey();
    String name = CommonUtils.randomString();
    String document = CommonUtils.randomString();
    String unit = CommonUtils.randomString();
    long value = CommonUtils.current();
    long startTime = CommonUtils.current();
    try (Counter counter =
        Counter.builder()
            .key(key)
            .item(name)
            .value(value)
            .startTime(startTime)
            .document(document)
            .unit(unit)
            .build()) {
      Assertions.assertEquals(key, counter.key());
      Assertions.assertEquals(name, counter.item());
      Assertions.assertEquals(document, counter.getDocument());
      Assertions.assertEquals(value, counter.getValue());
      Assertions.assertEquals(startTime, counter.getStartTime());
      Assertions.assertEquals(unit, counter.getUnit());
    }
  }
}
