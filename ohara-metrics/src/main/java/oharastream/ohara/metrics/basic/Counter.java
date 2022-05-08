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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.ReleaseOnce;
import oharastream.ohara.metrics.BeanChannel;

/**
 * This class is an implementation of JMX Bean. We need to implement serializable since we write
 * metrics data to rocksDB.
 */
public final class Counter extends ReleaseOnce implements CounterMBean {

  public static Builder builder() {
    return new Builder();
  }

  @VisibleForTesting final boolean needClose;
  @VisibleForTesting final Map<String, String> properties;
  private final ObjectKey key;
  private final String item;
  private final String document;
  private final String unit;
  private final AtomicLong value = new AtomicLong(0);
  private final AtomicLong lastModified = new AtomicLong(CommonUtils.current());
  private final long startTime;
  private final long queryTime;

  private Counter(
      boolean needClose,
      Map<String, String> properties,
      ObjectKey key,
      String item,
      String document,
      String unit,
      long startTime,
      long queryTime,
      long value,
      long lastModified) {
    this.needClose = needClose;
    this.properties = Map.copyOf(CommonUtils.requireNonEmpty(properties));
    this.key = Objects.requireNonNull(key);
    this.item = CommonUtils.requireNonEmpty(item);
    this.document = CommonUtils.requireNonEmpty(document);
    this.unit = CommonUtils.requireNonEmpty(unit);
    this.startTime = startTime;
    this.queryTime = queryTime;
    this.value.set(value);
    this.lastModified.set(lastModified);
  }

  @Override
  public ObjectKey key() {
    return key;
  }

  @Override
  public String item() {
    return item;
  }

  @Override
  public String getDocument() {
    return document;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  private long updateLastModified() {
    return lastModified.updateAndGet(last -> Math.max(last, CommonUtils.current()));
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  public long incrementAndGet() {
    try {
      return value.incrementAndGet();
    } finally {
      updateLastModified();
    }
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  public long getAndIncrement() {
    try {
      return value.getAndIncrement();
    } finally {
      updateLastModified();
    }
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the updated value
   */
  public long decrementAndGet() {
    try {
      return value.decrementAndGet();
    } finally {
      updateLastModified();
    }
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the previous value
   */
  public long getAndDecrement() {
    try {
      return value.getAndDecrement();
    } finally {
      updateLastModified();
    }
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  public long addAndGet(long delta) {
    try {
      return value.addAndGet(delta);
    } finally {
      updateLastModified();
    }
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  public long getAndAdd(long delta) {
    try {
      return value.getAndAdd(delta);
    } finally {
      updateLastModified();
    }
  }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public long getAndSet(long newValue) {
    try {
      return value.getAndSet(newValue);
    } finally {
      updateLastModified();
    }
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   * @return the new value
   */
  public long setAndGet(long newValue) {
    try {
      value.set(newValue);
      return newValue;
    } finally {
      updateLastModified();
    }
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getQueryTime() {
    return queryTime;
  }

  @Override
  public long getLastModified() {
    return lastModified.get();
  }

  @Override
  public long getValue() {
    return value.get();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Counter) {
      Counter another = (Counter) obj;
      return another.key().equals(key())
          && another.item().equals(item())
          && another.getStartTime() == getStartTime()
          && another.getValue() == getValue()
          && another.getUnit().equals(getUnit())
          && another.getQueryTime() == getQueryTime()
          && another.getLastModified() == getLastModified();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key(), item(), getValue(), getStartTime(), getUnit(), getLastModified());
  }

  @Override
  public String toString() {
    return "key:"
        + key()
        + " item:"
        + item()
        + " start:"
        + getStartTime()
        + " value:"
        + getValue()
        + " unit:"
        + getUnit()
        + " query time:"
        + getQueryTime()
        + " last modified:"
        + getLastModified();
  }

  @Override
  protected void doClose() {
    if (needClose) BeanChannel.unregister(CounterMBean.DOMAIN, properties);
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<Counter> {
    private String id;
    private ObjectKey key;
    private String item;
    private String unit = "N/A";
    private String document = "there is no document for this counter...";
    private long value = 0;
    private long startTime = CommonUtils.current();
    private long lastModified = startTime;
    private long queryTime = CommonUtils.current();

    private Builder() {}

    @Optional("default is random string")
    public Builder id(String id) {
      this.id = CommonUtils.requireNonEmpty(id);
      return this;
    }

    public Builder key(ObjectKey key) {
      this.key = Objects.requireNonNull(key);
      return this;
    }

    public Builder item(String item) {
      this.item = CommonUtils.requireNonEmpty(item);
      return this;
    }

    @Optional("default is zero")
    public Builder value(long value) {
      this.value = value;
      return this;
    }

    @Optional("default is current time")
    Builder startTime(long startTime) {
      this.startTime = CommonUtils.requirePositiveLong(startTime);
      return this;
    }

    @Optional("default is current time")
    Builder lastModified(long lastModified) {
      this.lastModified = CommonUtils.requirePositiveLong(lastModified);
      return this;
    }

    @Optional("default is current time")
    Builder queryTime(long queryTime) {
      this.queryTime = CommonUtils.requirePositiveLong(queryTime);
      return this;
    }

    @Optional("default is no document")
    public Builder document(String document) {
      this.document = CommonUtils.requireNonEmpty(document);
      return this;
    }

    @Optional("default is N/A")
    public Builder unit(String unit) {
      this.unit = CommonUtils.requireNonEmpty(unit);
      return this;
    }

    private void checkArgument() {
      Objects.requireNonNull(key);
      CommonUtils.requireNonEmpty(item);
    }

    /**
     * create a mutable counter. NOTED: this method is NOT public since we disallow user to create a
     * counter without registry.
     *
     * @return Counter
     */
    @Override
    public Counter build() {
      return build(false);
    }

    /**
     * create and register a mutable counter.
     *
     * @return Counter
     */
    public Counter register() {
      Counter counter = build(true);
      return BeanChannel.<Counter>register()
          .domain(DOMAIN)
          .properties(counter.properties)
          .beanObject(counter)
          .run();
    }

    /**
     * Create a counter with a flag indicating the action of unregistering beans from local jvm.
     *
     * @param needClose if true, the close() method will invoke unregister also.
     * @return counter
     */
    private Counter build(boolean needClose) {
      checkArgument();
      var properties =
          Map.of(
              TYPE_KEY,
              TYPE_VALUE,
              // the metrics tools (for example, jmc) can distinguish the counter via the name.
              KEY_KEY,
              key.toPlain(),
              // the metrics tools (for example, jmc) can distinguish the counter via the name.
              ITEM_KEY,
              item,
              // we use a random string to avoid duplicate jmx
              // This property is required since kafka worker may create multiple tasks on same
              // worker node.
              // If we don't have this id, the multiple tasks will fail since the duplicate
              // counters.
              ID_KEY,
              CommonUtils.isEmpty(id) ? CommonUtils.randomString() : id);
      return new Counter(
          needClose,
          properties,
          key,
          item,
          document,
          unit,
          startTime,
          queryTime,
          value,
          lastModified);
    }
  }
}
