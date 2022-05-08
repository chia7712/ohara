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

package oharastream.ohara.common.util;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Releasable extends AutoCloseable {
  /** ohara doesn't use checked exception. */
  @Override
  void close();

  Logger LOG = LoggerFactory.getLogger(Releasable.class);

  /**
   * this helper method close object if it is not null.
   *
   * @param obj releasable object
   */
  static void close(AutoCloseable obj) {
    close(obj, t -> LOG.error("failed to release object:" + obj, t));
  }

  /**
   * this helper method close object if it is not null.
   *
   * @param obj releasable object
   * @param consumer handle the exception
   */
  static void close(AutoCloseable obj, Consumer<Throwable> consumer) {
    try {
      if (obj != null) obj.close();
    } catch (Throwable e) {
      consumer.accept(e);
    }
  }
}
