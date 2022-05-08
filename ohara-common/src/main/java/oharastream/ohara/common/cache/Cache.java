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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.util.CommonUtils;

/**
 * A wrap of google guava.Caching. Guava offers a powerful local cache, which is good to ohara to
 * speed up some slow data access. However, using guava cache in whole ohara is overkill so we wrap
 * it to offer a more simple version to other modules. In this wrap, we offer two kind of behavior
 * of getting data from cache.
 *
 * <p>Noted: the getter facing the expired data first is blocked until the data is refreshed
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Cache<K, V> {

  /**
   * return the value associated to the input key. The loading will happen if there is no value.
   *
   * @param key key
   * @return value
   */
  V get(K key);

  /**
   * snapshot all cached key-value pairs
   *
   * @return a unmodified map
   */
  Map<K, V> snapshot();

  /**
   * update the key-value stored in this cache. the previous value will be replaced.
   *
   * @param key key
   * @param value new value
   */
  default void put(K key, V value) {
    put(Map.of(key, value));
  }

  /**
   * update the key-values stored in this cache. the previous values will be replaced.
   *
   * @param map keys-newValues
   */
  void put(Map<? extends K, ? extends V> map);

  /** @return the approximate number of this cache. */
  long size();

  /** Remove all entries in this cache. */
  void clear();

  static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  class Builder<K, V> implements oharastream.ohara.common.pattern.Builder<Cache<K, V>> {
    private int maxSize = 1000;
    private Duration timeout = Duration.ofSeconds(5);
    private Function<K, V> fetcher = null;

    private Builder() {}

    @Optional("Default value is 1000")
    public Builder<K, V> maxSize(int maxSize) {
      this.maxSize = CommonUtils.requirePositiveInt(maxSize);
      return this;
    }

    @Optional("Default value is 5 seconds")
    public Builder<K, V> timeout(Duration timeout) {
      this.timeout = Objects.requireNonNull(timeout);
      return this;
    }

    public Builder<K, V> fetcher(Function<K, V> fetcher) {
      this.fetcher = Objects.requireNonNull(fetcher);
      return this;
    }

    @Override
    public Cache<K, V> build() {
      Objects.requireNonNull(fetcher);
      return new Cache<>() {
        private final LoadingCache<K, V> cache =
            CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .build(
                    new CacheLoader<>() {
                      @Override
                      public V load(K key) {
                        return fetcher.apply(key);
                      }
                    });

        @Override
        public V get(K key) {
          try {
            return cache.get(Objects.requireNonNull(key));
          } catch (ExecutionException e) {
            if (e.getCause() != null) throw new IllegalStateException(e.getCause());
            else throw new IllegalStateException(e);
          }
        }

        @Override
        public Map<K, V> snapshot() {
          return Map.copyOf(cache.asMap());
        }

        @Override
        public void put(Map<? extends K, ? extends V> map) {
          cache.putAll(map);
        }

        @Override
        public long size() {
          return cache.size();
        }

        @Override
        public void clear() {
          cache.invalidateAll();
        }
      };
    }
  }
}
