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

package oharastream.ohara.kafka;

import java.util.Arrays;
import java.util.Objects;

public class Header {
  public static final String SOURCE_CLASS_KEY = "SOURCE_CLASS";
  public static final String SOURCE_KEY_KEY = "SOURCE_KEY";

  private final String key;
  private final byte[] value;

  Header(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public String key() {
    return key;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Header header = (Header) o;
    return Objects.equals(key, header.key) && Arrays.equals(value, header.value);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "Header{" + "key='" + key + '\'' + ", value=" + Arrays.toString(value) + '}';
  }
}
