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

/** expose the inner byte array so we don't need to clone whole array. */
class ByteArrayOutputStream extends java.io.ByteArrayOutputStream {
  ByteArrayOutputStream(int capacity) {
    super(capacity);
  }

  /**
   * return inner byte array if it is full. Otherwise, an new array clone from inner array is
   * returned.
   *
   * @return byte array
   */
  @Override
  public synchronized byte[] toByteArray() {
    if (buf == null) return new byte[0];
    else if (count == buf.length) return buf;
    else return super.toByteArray();
  }
}
