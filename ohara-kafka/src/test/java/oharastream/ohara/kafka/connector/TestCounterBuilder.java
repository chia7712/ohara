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

package oharastream.ohara.kafka.connector;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.metrics.basic.Counter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCounterBuilder extends OharaTest {

  @Test
  public void testNullGroup() {
    Assertions.assertThrows(NullPointerException.class, () -> CounterBuilder.of().key(null));
  }

  @Test
  public void testNullName() {
    Assertions.assertThrows(NullPointerException.class, () -> CounterBuilder.of().name(null));
  }

  @Test
  public void testEmptyName() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> CounterBuilder.of().name(""));
  }

  @Test
  public void testNullUnit() {
    Assertions.assertThrows(NullPointerException.class, () -> CounterBuilder.of().unit(null));
  }

  @Test
  public void testEmptyUnit() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> CounterBuilder.of().unit(""));
  }

  @Test
  public void testNullDocument() {
    Assertions.assertThrows(NullPointerException.class, () -> CounterBuilder.of().document(null));
  }

  @Test
  public void testEmptyDocument() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> CounterBuilder.of().document(""));
  }

  @Test
  public void testSimpleBuild() {
    ObjectKey key = ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString());
    String name = CommonUtils.randomString();
    String unit = CommonUtils.randomString();
    String document = CommonUtils.randomString();
    Counter counter = CounterBuilder.of().key(key).name(name).unit(unit).document(document).build();
    Assertions.assertEquals(key, counter.key());
    Assertions.assertEquals(name, counter.item());
    Assertions.assertEquals(unit, counter.getUnit());
    Assertions.assertEquals(document, counter.getDocument());
  }
}
