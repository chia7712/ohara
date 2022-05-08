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

package oharastream.ohara.common.pattern;

/**
 * This is do-nothing but base class. The purpose of this class is used to make all builder have
 * similar signatures.
 *
 * @param <T> type of final result
 */
public interface Builder<T> {
  /**
   * produce a instance. This is the final operation of chain of this builder. DON'T reuse this
   * builder since all inner objects are undefined after instantiating a object.
   *
   * @return an object
   */
  T build();
}
