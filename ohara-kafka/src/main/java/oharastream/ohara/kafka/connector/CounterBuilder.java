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

import oharastream.ohara.common.pattern.Builder;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.metrics.basic.Counter;

/**
 * this is a wrap of Counter.Builder. It simplify the Counter.Builder via removing some methods
 * which are unrelated to connector.
 */
public final class CounterBuilder implements Builder<Counter> {

  static CounterBuilder of() {
    return new CounterBuilder();
  }

  private final Counter.Builder builder = Counter.builder();

  private CounterBuilder() {}

  /**
   * We hind group from connector developer since the group must be the id of connector. Otherwise,
   * Ohara Configurator can't fetch metrics for specific connectors.
   *
   * @param key it must be equal with connector's key
   * @return this builder
   */
  CounterBuilder key(ObjectKey key) {
    builder.key(key);
    return this;
  }

  public CounterBuilder name(String name) {
    builder.item(name);
    return this;
  }

  public CounterBuilder unit(String unit) {
    builder.unit(unit);
    return this;
  }

  public CounterBuilder document(String document) {
    builder.document(document);
    return this;
  }

  /**
   * create and register an new counter.
   *
   * @return an new counter
   */
  @Override
  public Counter build() {
    return builder.register();
  }
}
