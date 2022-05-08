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

package oharastream.ohara.common.cache;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRefreshableCache extends OharaTest {

  @Test
  public void nullFrequency() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> RefreshableCache.<String, String>builder().frequency(null));
  }

  @Test
  public void nullTimeout() {
    Assertions.assertThrows(
        NullPointerException.class, () -> RefreshableCache.<String, String>builder().timeout(null));
  }

  @Test
  public void nullRemoveListener() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> RefreshableCache.<String, String>builder().preRemoveObserver(null));
  }

  @Test
  public void nullSupplier() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> RefreshableCache.<String, String>builder().supplier(null));
  }

  @Test
  public void testDefaultBuilder() {
    RefreshableCache.<String, String>builder()
        .supplier(() -> Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
        .build()
        .close();
  }

  @Test
  public void testAutoRefresh() throws InterruptedException {
    AtomicInteger supplierCount = new AtomicInteger(0);
    String newKey = CommonUtils.randomString();
    String newValue = CommonUtils.randomString();
    try (RefreshableCache<String, String> cache =
        RefreshableCache.<String, String>builder()
            .supplier(
                () -> {
                  supplierCount.incrementAndGet();
                  return Map.of(newKey, newValue);
                })
            .frequency(Duration.ofSeconds(2))
            .build()) {
      Assertions.assertEquals(0, supplierCount.get());
      TimeUnit.SECONDS.sleep(3);
      // ok, the cache is auto-refreshed
      Assertions.assertEquals(1, supplierCount.get());
      Assertions.assertEquals(newValue, cache.get(newKey).get());
      Assertions.assertEquals(1, cache.size());
      // insert a key-value
      cache.put(CommonUtils.randomString(), CommonUtils.randomString());
      Assertions.assertEquals(2, cache.size());
      // ok, the cache is auto-refreshed again
      TimeUnit.SECONDS.sleep(2);
      Assertions.assertTrue(supplierCount.get() >= 2);
      Assertions.assertEquals(newValue, cache.get(newKey).get());
      // the older keys are removed
      Assertions.assertEquals(1, cache.size());
    }
  }

  @Test
  public void testPutsAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> cache.put(Map.of(CommonUtils.randomString(), CommonUtils.randomString())));
  }

  @Test
  public void testRequestUpdate() throws InterruptedException {
    AtomicInteger count = new AtomicInteger(0);
    RefreshableCache<String, String> cache =
        RefreshableCache.<String, String>builder()
            .supplier(
                () -> {
                  count.incrementAndGet();
                  return Map.of(CommonUtils.randomString(), CommonUtils.randomString());
                })
            // no update before we all die
            .frequency(Duration.ofDays(10000000))
            .build();

    cache.get(CommonUtils.randomString());
    Assertions.assertEquals(0, count.get());
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(0, count.get());
    cache.requestUpdate();
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(1, count.get());
  }

  @Test
  public void testClear() throws InterruptedException {
    RefreshableCache<String, String> cache =
        RefreshableCache.<String, String>builder()
            .supplier(() -> Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
            // no update before we all die
            .frequency(Duration.ofDays(10000000))
            .build();
    cache.requestUpdate();
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(1, cache.size());
    cache.clear();
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  public void testSnapshotAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(IllegalStateException.class, cache::snapshot);
  }

  @Test
  public void testClearAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(IllegalStateException.class, cache::clear);
  }

  @Test
  public void testRequestUpdateAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(IllegalStateException.class, cache::requestUpdate);
  }

  @Test
  public void testSizeAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(IllegalStateException.class, cache::size);
  }

  @Test
  public void testGetAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(
        IllegalStateException.class, () -> cache.get(CommonUtils.randomString()));
  }

  @Test
  public void testPutAfterClose() {
    RefreshableCache<String, String> cache = cache();
    cache.close();
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> cache.put(CommonUtils.randomString(), CommonUtils.randomString()));
  }

  @Test
  public void testRemove() {
    RefreshableCache<String, String> cache = cache();
    String key = CommonUtils.randomString();
    String value = CommonUtils.randomString();
    cache.put(key, value);
    Assertions.assertEquals(value, cache.get(key).get());
    cache.remove(key);
    Assertions.assertFalse(cache.get(key).isPresent());
  }

  @Test
  public void testTimeout() throws InterruptedException {
    RefreshableCache<String, String> cache =
        RefreshableCache.<String, String>builder()
            .supplier(() -> Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
            .frequency(Duration.ofSeconds(1000))
            .frequency(Duration.ofSeconds(2))
            .build();
    String key = CommonUtils.randomString();
    String value = CommonUtils.randomString();
    cache.put(key, value);
    Assertions.assertEquals(value, cache.get(key).get());
    TimeUnit.SECONDS.sleep(3);
    Assertions.assertFalse(cache.get(key).isPresent());
  }

  @Test
  public void testMissData() throws InterruptedException {
    String key = CommonUtils.randomString();
    String value = CommonUtils.randomString();
    Map<String, String> data =
        Map.of(
            key,
            value,
            CommonUtils.randomString(),
            CommonUtils.randomString(),
            CommonUtils.randomString(),
            CommonUtils.randomString(),
            CommonUtils.randomString(),
            CommonUtils.randomString());
    AtomicInteger updateCount = new AtomicInteger(0);
    try (RefreshableCache<String, String> cache =
        RefreshableCache.<String, String>builder()
            .supplier(
                () -> {
                  updateCount.incrementAndGet();
                  return data;
                })
            .frequency(Duration.ofMillis(500))
            .build()) {
      cache.put(data);
      int testTime = 10; // seconds
      int numberOfThreads = 6;
      AtomicBoolean closed = new AtomicBoolean(false);
      AtomicInteger missCount = new AtomicInteger(0);
      ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
      IntStream.range(0, numberOfThreads)
          .forEach(
              index ->
                  service.execute(
                      () -> {
                        try {
                          while (!closed.get()) {
                            if (cache.get(key).isEmpty()) missCount.incrementAndGet();
                            TimeUnit.MILLISECONDS.sleep(100);
                          }
                        } catch (InterruptedException e) {
                          // nothing
                        }
                      }));
      try {
        TimeUnit.SECONDS.sleep(testTime);
      } finally {
        closed.set(true);
        service.shutdownNow();
        Assertions.assertTrue(service.awaitTermination(testTime, TimeUnit.SECONDS));
        Assertions.assertTrue(updateCount.get() > 0);
        Assertions.assertEquals(0, missCount.get());
      }
    }
  }

  @Test
  public void testRemoveListener() throws InterruptedException {
    AtomicInteger updateCount = new AtomicInteger(0);
    String key = CommonUtils.randomString();
    String value = CommonUtils.randomString();
    try (RefreshableCache<String, String> cache =
        RefreshableCache.<String, String>builder()
            .supplier(
                () -> {
                  updateCount.incrementAndGet();
                  return Map.of();
                })
            .frequency(Duration.ofSeconds(1))
            .preRemoveObserver((k, v) -> !key.equals(k))
            .build()) {
      cache.put(key, value);
      TimeUnit.SECONDS.sleep(2);
      Assertions.assertTrue(updateCount.get() > 0);
      Assertions.assertEquals(cache.get(key).get(), value);
    }
  }

  private static RefreshableCache<String, String> cache() {
    return RefreshableCache.<String, String>builder()
        .supplier(() -> Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
        .frequency(Duration.ofSeconds(2))
        .build();
  }
}
