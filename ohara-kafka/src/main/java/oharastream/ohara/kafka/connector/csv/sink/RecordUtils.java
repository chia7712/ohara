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

package oharastream.ohara.kafka.connector.csv.sink;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.kafka.connector.RowSinkRecord;

public class RecordUtils {

  public static boolean isNonEmpty(String str) {
    return str != null && str.length() > 0;
  }

  public static String toHeader(List<Column> newSchema) {
    return newSchema.stream()
        .sorted(Comparator.comparingInt(Column::order))
        .map(s -> s.newName())
        .collect(Collectors.joining(","));
  }

  public static String toLine(List<Column> newSchema, RowSinkRecord record) {
    return newSchema.stream()
        .sorted(Comparator.comparing(Column::order))
        .flatMap(
            column -> {
              if (column.dataType() == DataType.BYTES) {
                throw new RuntimeException(
                    "CSV sink connector not support type: " + column.dataType());
              }
              return record.row().cells().stream()
                  .filter(cell -> column.name().equals(cell.name()));
            })
        .map(cell -> cell.value().toString())
        .collect(Collectors.joining(","));
  }

  public static List<Column> newSchema(List<Column> schema, RowSinkRecord record) {
    if (schema != null && !schema.isEmpty()) return schema;
    return record.row().cells().stream()
        .map(cell -> Column.builder().name(cell.name()).dataType(DataType.OBJECT).order(0).build())
        .collect(Collectors.toUnmodifiableList());
  }
}
