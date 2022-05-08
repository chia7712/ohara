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

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceTaskContext;

/** a wrap to kafka SourceTaskContext */
public interface RowSourceContext {

  /**
   * Get the offset for the specified partition. If the data isn't already available locally, this
   * gets it from the backing store, which may require some network round trips.
   *
   * @param partition object uniquely identifying the partition from data
   * @param <T> type of offset value
   * @return object uniquely identifying the offset in the partition from data
   */
  <T> Map<String, Object> offset(Map<String, T> partition);

  /**
   * Get a set from offsets for the specified partition identifiers. This may be more efficient than
   * calling offset() repeatedly.
   *
   * <p>Note that when errors occur, this method omits the associated data and tries to return as
   * many from the requested values as possible. This allows a task that's managing many partitions
   * to still proceed with any available data. Therefore, implementations should take care to check
   * that the data is actually available in the returned response. The only case when an exception
   * will be thrown is if the entire request failed, e.g. because the underlying storage was
   * unavailable.
   *
   * @param partitions set from identifiers for partitions from data
   * @param <T> type of offset value
   * @return a map from partition identifiers to decoded offsets
   */
  <T> Map<Map<String, T>, Map<String, Object>> offset(List<Map<String, T>> partitions);

  static RowSourceContext toRowSourceContext(SourceTaskContext context) {
    return new RowSourceContext() {

      @Override
      public <T> Map<String, Object> offset(Map<String, T> partition) {

        Map<String, Object> r = context.offsetStorageReader().offset(partition);
        return r == null ? Map.of() : r;
      }

      @Override
      public <T> Map<Map<String, T>, Map<String, Object>> offset(List<Map<String, T>> partitions) {

        return context.offsetStorageReader().offsets(partitions);
      }
    };
  }
}
