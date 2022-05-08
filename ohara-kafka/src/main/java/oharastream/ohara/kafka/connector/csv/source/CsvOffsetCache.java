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

import java.util.HashMap;
import java.util.Map;
import oharastream.ohara.kafka.connector.RowSourceContext;

public class CsvOffsetCache implements OffsetCache {
  private final Map<String, Integer> cache = new HashMap<>();

  public void loadIfNeed(RowSourceContext context, String path) {
    /**
     * RowSourceContext is based on kafka context, and the Getter methods is based on kafka topic.
     * It means touching the method may be a high latency operation since it produces network
     * traffics. Therefore, we SHOULD avoid triggering the method frequently.
     */
    if (!cache.containsKey(path)) {
      Map<String, Object> offset =
          context.offset(Map.of(CsvRecordConverter.CSV_PARTITION_KEY, path));
      if (!offset.isEmpty()) update(path, getOffsetValue(offset));
    }
  }

  public void update(String path, int index) {
    if (!cache.containsKey(path)) {
      cache.put(path, index);
    }
    int previous = cache.get(path);
    if (index > previous) cache.put(path, index);
  }

  public boolean predicate(String path, int index) {
    if (cache.containsKey(path)) {
      int previous = cache.get(path);
      return index > previous;
    }
    return true;
  }

  private int getOffsetValue(Map<String, Object> offset) {
    Object value = offset.get(CsvRecordConverter.CSV_OFFSET_KEY);
    if (value instanceof Short) {
      return ((Short) value).intValue();
    } else if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    } else if (value instanceof Double) {
      return ((Double) value).intValue();
    }
    return Integer.parseInt((String) value);
  }
}
