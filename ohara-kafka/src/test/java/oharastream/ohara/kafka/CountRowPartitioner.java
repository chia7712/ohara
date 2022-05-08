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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.setting.TopicKey;

public class CountRowPartitioner extends RowPartitioner {
  public final AtomicInteger count = new AtomicInteger(0);

  @Override
  public Optional<Integer> partition(
      TopicKey topicKey, Row row, byte[] serializedRow, Cluster cluster) {
    count.incrementAndGet();
    return Optional.empty();
  }
}
