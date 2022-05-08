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

import java.util.List;
import java.util.Map;
import oharastream.ohara.kafka.connector.RowSourceContext;

public class FakeSourceContext implements RowSourceContext {
  public static FakeSourceContext of() {
    return new FakeSourceContext();
  }

  @Override
  public <T> Map<String, Object> offset(Map<String, T> partition) {
    return Map.of();
  }

  @Override
  public <T> Map<Map<String, T>, Map<String, Object>> offset(List<Map<String, T>> partitions) {
    return Map.of();
  }
}
