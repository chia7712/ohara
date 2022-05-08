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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache with auto-refresh function. It invokes a inner thread to loop the refresh function, which
 * offers the latest key-values to the cache. The use case is that you really really really hate the
 * wait of reloading the new value for timeout key. The inner thread keeps feeding the thread on the
 * key-values supplied by you. Noted that it doesn't guarantee that all your get call won't be
 * blocked anymore since the first call submitted by you is still blocked. Noted that {@link
 * RefreshableCache.Builder#supplier} will clean up all cached data and then pull all generated data
 * from to cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface RefreshableCache<K, V> extends Releasable {
  /**
   * remove the cached value associated to key.
   *
   * @param key key
   */
  void remove(K key);

  /**
   * return the value associated to the input key.
   *
   * @param key key
   * @return value
   */
  Optional<V> get(K key);

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

  /**
   * The inner time-based auto-refresher is enough to most use cases. However, we are always in a
   * situation that we should update the cache right now. This method save your life that you can
   * request the inner thread to update the cache. Noted, the method doesn't block your thread since
   * what it does is to send a request without any wait.
   */
  void requestUpdate();

  Logger LOG = LoggerFactory.getLogger(RefreshableCache.class);

  static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  class Builder<K, V> implements oharastream.ohara.common.pattern.Builder<RefreshableCache<K, V>> {
    private int maxSize = 1000;
    private Duration timeout = null;
    private Duration frequency = Duration.ofSeconds(5);
    private Supplier<Map<K, V>> supplier = null;
    /** the default value accept all remove request. */
    private BiFunction<K, V, Boolean> preRemoveObserver = (k, v) -> true;

    private Builder() {}

    @oharastream.ohara.common.annotations.Optional("Default value is 1000")
    public Builder<K, V> maxSize(int maxSize) {
      this.maxSize = CommonUtils.requirePositiveInt(maxSize);
      return this;
    }

    public Builder<K, V> supplier(Supplier<Map<K, V>> supplier) {
      this.supplier = Objects.requireNonNull(supplier);
      return this;
    }

    /**
     * This function is invoked when cache prepare to remove the data. Through this function, you
     * can save your data from the update process.
     *
     * @param preRemoveObserver The data is kept in cache if the function return false. Otherwise,
     *     the data may be removed from cache in updating.
     * @return this builder
     */
    @oharastream.ohara.common.annotations.Optional(
        "default is a function which always say yes to remove data from cache")
    public Builder<K, V> preRemoveObserver(BiFunction<K, V, Boolean> preRemoveObserver) {
      this.preRemoveObserver = Objects.requireNonNull(preRemoveObserver);
      return this;
    }

    /**
     * The time to remove cached entry automatically. If you ignore this option, the cached data
     * will be removed by auto-refresher only
     *
     * @param timeout timeout
     * @return this builder
     */
    @oharastream.ohara.common.annotations.Optional("default value is no timeout")
    public Builder<K, V> timeout(Duration timeout) {
      this.timeout = Objects.requireNonNull(timeout);
      return this;
    }

    /**
     * @param frequency the time to update cache
     * @return this builder
     */
    @oharastream.ohara.common.annotations.Optional("default value is 5 seconds")
    public Builder<K, V> frequency(Duration frequency) {
      this.frequency = Objects.requireNonNull(frequency);
      return this;
    }

    @Override
    public RefreshableCache<K, V> build() {
      Objects.requireNonNull(supplier);
      Objects.requireNonNull(frequency);
      Objects.requireNonNull(preRemoveObserver);
      com.google.common.cache.Cache<K, V> cache =
          timeout == null
              ? CacheBuilder.newBuilder().maximumSize(maxSize).build()
              : CacheBuilder.newBuilder()
                  .maximumSize(maxSize)
                  .expireAfterWrite(timeout.toMillis(), TimeUnit.MILLISECONDS)
                  .build();
      ExecutorService service = Executors.newSingleThreadExecutor();
      AtomicBoolean closed = new AtomicBoolean(false);
      BlockingQueue<Boolean> queue = new ArrayBlockingQueue<>(1);
      service.execute(
          () -> {
            while (!closed.get()) {
              try {
                // we use wait/notify instead of TimeUnit.sleep since we enable caller to wake up
                // this inner thread
                // to update the cache.
                queue.poll(frequency.toMillis(), TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                closed.set(true);
                break;
              }
              try {
                // DON'T clear cache in first phase since the supplier may fail
                Map<K, V> data = supplier.get();
                Map<K, V> oldData = new HashMap<>(cache.asMap());
                oldData.forEach(
                    (key, value) -> {
                      if (!data.containsKey(key) && preRemoveObserver.apply(key, value))
                        cache.invalidate(key);
                    });
                cache.putAll(data);
              } catch (Throwable e) {
                LOG.error("failed to update cache", e);
              }
            }
            LOG.info("refreshable cache is gone");
          });
      return new RefreshableCache<K, V>() {

        @Override
        public void requestUpdate() {
          if (closed.get()) throw new IllegalStateException("cache is closed!!!");
          // we don't care for the return value since the false means that another thread invoke a
          // update request at the same time.
          queue.offer(true);
        }

        @Override
        public void close() {
          if (closed.compareAndSet(false, true)) {
            service.shutdownNow();
            try {
              if (!service.awaitTermination(30, TimeUnit.SECONDS))
                throw new IllegalStateException("failed to release cache");
            } catch (InterruptedException e) {
              throw new IllegalStateException("failed to release cache", e);
            }
          }
        }

        @Override
        public void remove(K key) {
          cache.invalidate(key);
        }

        @Override
        public Optional<V> get(K key) {
          if (closed.get()) throw new IllegalStateException("cache is closed!!!");
          return Optional.ofNullable(cache.getIfPresent(key));
        }

        @Override
        public Map<K, V> snapshot() {
          if (closed.get()) throw new IllegalStateException("cache is closed!!!");
          return Map.copyOf(cache.asMap());
        }

        @Override
        public void put(Map<? extends K, ? extends V> map) {
          if (closed.get()) throw new IllegalStateException("cache is closed!!!");
          cache.putAll(map);
        }

        @Override
        public long size() {
          if (closed.get()) throw new IllegalStateException("cache is closed!!!");
          return cache.size();
        }

        @Override
        public void clear() {
          if (closed.get()) throw new IllegalStateException("cache is closed!!!");
          cache.invalidateAll();
        }
      };
    }
  }
}
