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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.metrics.BeanChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCounter extends OharaTest {

  @Test
  public void testIncrementAndGet() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(1, counter.incrementAndGet());
    }
  }

  @Test
  public void testGetAndIncrement() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(0, counter.getAndIncrement());
    }
  }

  @Test
  public void testDecrementAndGet() {
    Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build();
    Assertions.assertEquals(-1, counter.decrementAndGet());
  }

  @Test
  public void testGetAndDecrement() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(0, counter.getAndDecrement());
    }
  }

  @Test
  public void testGetAndSet() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(0, counter.getAndSet(10));
    }
  }

  @Test
  public void testSetAndGet() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(10, counter.setAndGet(10));
    }
  }

  @Test
  public void testGetAndAdd() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(0, counter.getAndAdd(10));
    }
  }

  @Test
  public void testAddAndGet() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(0)
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(10, counter.addAndGet(10));
    }
  }

  @Test
  public void testEquals() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(counter, counter);
    }
  }

  @Test
  public void testNotEqualsOnDiffGroup() {
    Counter.Builder builder =
        Counter.builder()
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10))
            .key(CommonUtils.randomKey());
    try (Counter c0 = builder.build();
        Counter c1 = builder.key(CommonUtils.randomKey()).build()) {
      Assertions.assertNotEquals(c0, c1);
    }
  }

  @Test
  public void testHashCodesOnDiffGroup() {
    Counter.Builder builder =
        Counter.builder()
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10))
            .key(CommonUtils.randomKey());
    try (Counter c0 = builder.build();
        Counter c1 = builder.key(CommonUtils.randomKey()).build()) {
      Assertions.assertNotEquals(c0.hashCode(), c1.hashCode());
    }
  }

  @Test
  public void testEqualsOnDiffDocument() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.document(CommonUtils.randomString()).build()) {
      Assertions.assertEquals(c0, c1);
    }
  }

  @Test
  public void testHashCodesOnDiffDocument() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.document(CommonUtils.randomString()).build()) {
      Assertions.assertEquals(c0.hashCode(), c1.hashCode());
    }
  }

  @Test
  public void testNotEqualsOnDiffUnit() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.unit(CommonUtils.randomString()).build()) {
      Assertions.assertNotEquals(c0, c1);
    }
  }

  @Test
  public void testHashCodesOnDiffUnit() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.unit(CommonUtils.randomString()).build()) {
      Assertions.assertNotEquals(c0.hashCode(), c1.hashCode());
    }
  }

  @Test
  public void testNotEqualsOnDiffValue() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.value(CommonUtils.current() + 1000).build()) {
      Assertions.assertNotEquals(c0, c1);
    }
  }

  @Test
  public void testHashCodesOnDiffValue() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.value(CommonUtils.current() + 1000).build()) {
      Assertions.assertNotEquals(c0.hashCode(), c1.hashCode());
    }
  }

  @Test
  public void testHashCode() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10))
            .build()) {
      Assertions.assertEquals(counter.hashCode(), counter.hashCode());
    }
  }

  @Test
  public void testNotEqualsOnName() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.item(CommonUtils.randomString(10)).build()) {
      Assertions.assertNotEquals(c0, c1);
    }
  }

  @Test
  public void testDiffHashCodeOnName() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c0 = builder.build();
        Counter c1 = builder.item(CommonUtils.randomString(10)).build()) {
      Assertions.assertNotEquals(c0.hashCode(), c1.hashCode());
    }
  }

  /**
   * We generate a random id for each counter so it is ok to produce multiple counter in same jvm
   */
  @SuppressWarnings("try")
  @Test
  public void testDuplicateRegisterWithDiffId() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10));
    try (Counter c = builder.register();
        Counter c2 = builder.register()) {
      // good
    }
  }

  @Test
  public void testDuplicateRegister() {
    Counter.Builder builder =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .value(CommonUtils.current())
            .startTime(CommonUtils.current())
            .item(CommonUtils.randomString(10))
            .id(CommonUtils.randomString());
    builder.register();
    Assertions.assertThrows(IllegalArgumentException.class, builder::register);
  }

  @Test
  public void testFromBean() {
    ObjectKey key = CommonUtils.randomKey();
    String name = CommonUtils.randomString();
    String document = CommonUtils.randomString();
    String unit = CommonUtils.randomString();
    try (Counter counter =
        Counter.builder().key(key).item(name).document(document).unit(unit).register()) {
      List<CounterMBean> beans = BeanChannel.local().counterMBeans();
      Assertions.assertNotEquals(0, beans.size());
      CounterMBean bean = beans.stream().filter(c -> c.item().equals(name)).findFirst().get();
      Assertions.assertEquals(counter.key(), bean.key());
      Assertions.assertEquals(counter.item(), bean.item());
      Assertions.assertEquals(counter.getDocument(), bean.getDocument());
      Assertions.assertEquals(counter.getUnit(), bean.getUnit());
      Assertions.assertEquals(key, bean.key());
      Assertions.assertEquals(name, bean.item());
      Assertions.assertEquals(document, bean.getDocument());
      Assertions.assertEquals(unit, bean.getUnit());

      counter.setAndGet(CommonUtils.current());

      CommonUtils.await(
          () ->
              BeanChannel.local().counterMBeans().stream()
                      .filter(c -> c.item().equals(name))
                      .findFirst()
                      .get()
                      .getValue()
                  == counter.getValue(),
          java.time.Duration.ofSeconds(10));
    }
  }

  @Test
  public void testProperties() {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .item(CommonUtils.randomString(10))
            .register()) {
      Assertions.assertFalse(counter.properties.isEmpty());
      Assertions.assertTrue(counter.needClose);
    }
    try (Counter counter =
        Counter.builder().key(CommonUtils.randomKey()).item(CommonUtils.randomString(10)).build()) {
      Assertions.assertFalse(counter.properties.isEmpty());
      Assertions.assertFalse(counter.needClose);
    }
  }

  @Test
  public void testClose() {
    Map<String, String> properties;
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .item(CommonUtils.randomString(10))
            .register()) {
      Assertions.assertEquals(
          1,
          BeanChannel.builder()
              .local()
              .domainName(Counter.DOMAIN)
              .properties(counter.properties)
              .local()
              .build()
              .size());
      properties = counter.properties;
    }
    Assertions.assertEquals(
        0,
        BeanChannel.builder()
            .local()
            .domainName(Counter.DOMAIN)
            .properties(properties)
            .local()
            .build()
            .size());
  }

  @Test
  public void testZeroStartTime() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Counter.builder()
                .key(CommonUtils.randomKey())
                .item("name")
                .unit("unit")
                .document("document")
                .startTime(0)
                .build());
  }

  @Test
  public void testNegativeStartTime() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Counter.builder()
                .key(CommonUtils.randomKey())
                .item("name")
                .unit("unit")
                .document("document")
                .startTime(-999)
                .build());
  }

  @Test
  public void testZeroQueryTime() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            Counter.builder()
                .key(CommonUtils.randomKey())
                .item("name")
                .unit("unit")
                .document("document")
                .queryTime(0)
                .build());
  }

  @Test
  public void testNegativeQueryTime() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Counter.builder().queryTime(-999));
  }

  @Test
  public void testNegativeLastModifiedTime() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Counter.builder().lastModified(-999));
  }

  @Test
  public void testInPerSecond() throws InterruptedException {
    try (Counter counter =
        Counter.builder()
            .key(CommonUtils.randomKey())
            .item(CommonUtils.randomString(10))
            .register()) {
      TimeUnit.SECONDS.sleep(1);
      counter.addAndGet(100);
      Assertions.assertEquals(100, counter.getValue());
      Assertions.assertNotEquals(0.0F, counter.valueInPerSec());
    }
  }
}
