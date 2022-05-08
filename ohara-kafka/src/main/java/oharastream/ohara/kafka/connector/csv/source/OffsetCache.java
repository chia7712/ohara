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

package oharastream.ohara.kafka.connector.csv.source;

import oharastream.ohara.kafka.connector.RowSourceContext;

/** Used to manage the offset from files */
public interface OffsetCache {

  /**
   * if offset doesn't exist, lode the latest offset from RowSourceContext
   *
   * @param context kafka's cache
   * @param path file path
   */
  void loadIfNeed(RowSourceContext context, String path);

  /**
   * add (index, path) to the cache
   *
   * @param path file path
   * @param index index from line
   */
  void update(String path, int index);

  /**
   * check whether the index from path is processed.
   *
   * @param path file path
   * @param index index from line
   * @return true if the index from line isn't processed. otherwise, false
   */
  boolean predicate(String path, int index);
}
