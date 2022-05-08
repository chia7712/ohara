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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** This is a stream helper. */
public final class StreamUtils {

  /** Suppress default constructor for noninstantiability */
  private StreamUtils() {
    throw new AssertionError();
  }

  /**
   * Zips the specified stream with its indices.
   *
   * @param stream an {@link java.util.stream.Stream}
   * @param <T> the type of the stream elements
   * @return a stream consisting of the results with index
   */
  public static <T> Stream<Map.Entry<Integer, T>> zipWithIndex(Stream<? extends T> stream) {
    var count = new AtomicInteger(0);
    return stream.map(obj -> Map.entry(count.getAndIncrement(), obj));
  }
}
