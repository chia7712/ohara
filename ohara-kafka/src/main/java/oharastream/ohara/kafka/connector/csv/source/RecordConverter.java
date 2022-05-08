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
import java.util.stream.Stream;
import oharastream.ohara.kafka.connector.RowSourceRecord;

/** A record converter is used to convert the text lines to Kafka source records */
@FunctionalInterface
public interface RecordConverter {

  /**
   * Convert the text lines to records.
   *
   * <p>Depending on the situation, you can choose to handle IOException or throw an unchecked
   * exception.
   *
   * @param lines a stream of String
   * @return a array from RowSourceRecord. If nothing is readable (i.e EOF), an empty collection is
   *     returned.
   */
  List<RowSourceRecord> convert(Stream<String> lines);
}
