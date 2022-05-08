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

package oharastream.ohara.common.data;

import java.util.Objects;
import java.util.stream.IntStream;
import oharastream.ohara.common.util.ByteUtils;

/**
 * a basic data in ohara pipeline. Although no limit to the type in using Cell, serialization
 * exception may happen in transferring cell through network if the value is not serializable. see
 * {@link Serializer} for more information.
 *
 * @param <T> value type
 */
public interface Cell<T> {

  /** @return name from cell */
  String name();

  /** @return value from cell */
  T value();

  static <T> Cell<T> of(String name, T value) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(value);
    int hashCode;
    if (value instanceof byte[]) {
      byte[] bs = (byte[]) value;
      hashCode =
          name.hashCode()
              + 31
                  * IntStream.range(0, bs.length)
                      .map(i -> bs[i])
                      .reduce(1, (hash, current) -> hash * 31 + current);
    } else hashCode = name.hashCode() + 31 * value.hashCode();

    return new Cell<T>() {

      @Override
      public String name() {
        return name;
      }

      @Override
      public T value() {
        return value;
      }

      @Override
      public String toString() {
        return name() + "/" + value();
      }

      @Override
      public int hashCode() {
        return hashCode;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Cell) {
          Cell<?> that = (Cell<?>) obj;
          // java can't do deep comparison for byte array...
          if (value() instanceof byte[] && that.value() instanceof byte[])
            return name().equals(that.name())
                && ByteUtils.equals((byte[]) value(), (byte[]) that.value());
          return name().equals(that.name()) && value().equals(that.value());
        }
        return false;
      }
    };
  }
}
