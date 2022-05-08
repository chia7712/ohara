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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.*;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.StreamUtils;
import oharastream.ohara.kafka.connector.RowSourceRecord;

/**
 * A converter to be used to read data from a csv file, and convert to records of Kafka Connect
 * format
 */
public class CsvRecordConverter implements RecordConverter {
  @VisibleForTesting static final String CSV_REGEX = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
  public static final String CSV_PARTITION_KEY = "csv.file.path";
  public static final String CSV_OFFSET_KEY = "csv.file.line";

  public static Builder builder() {
    return new Builder();
  }

  private final String path;
  private final Set<TopicKey> topicKeys;
  private final List<Column> schema;

  private final Map<String, String> partition;
  private final OffsetCache cache;
  private final int maximumNumberOfLines;

  @Override
  public List<RowSourceRecord> convert(Stream<String> lines) {
    Map<Integer, List<Cell<String>>> cellsAndIndex = toCells(lines, maximumNumberOfLines);
    Map<Integer, Row> rowsAndIndex = transform(cellsAndIndex);
    List<RowSourceRecord> records = toRecords(rowsAndIndex);
    // ok. all data are prepared. let's update the cache
    rowsAndIndex.keySet().forEach(index -> cache.update(path, index));
    return records;
  }

  /**
   * read all lines from a reader, and then convert them to cells.
   *
   * @param lines the input stream
   * @param maximumNumberOfLines the max number of converted lines
   * @return data
   */
  @VisibleForTesting
  Map<Integer, List<Cell<String>>> toCells(Stream<String> lines, int maximumNumberOfLines) {
    Map<Integer, String> lineAndIndex =
        StreamUtils.zipWithIndex(lines)
            .filter(
                pair -> {
                  int index = pair.getKey();
                  if (index == 0) return true;
                  return cache.predicate(path, index);
                })
            // the header must be included.
            // increase maximumNumberOfLines only if it is smaller than Integer.MAX
            .limit(Math.max(maximumNumberOfLines, maximumNumberOfLines + 1))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    if (lineAndIndex.isEmpty()) return Map.of();

    String[] header =
        Arrays.stream(lineAndIndex.get(0).split(CSV_REGEX))
            .map(String::trim)
            .toArray(String[]::new);

    return lineAndIndex.entrySet().stream()
        .filter(e -> e.getKey() > 0)
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                e -> {
                  String line = e.getValue();
                  String[] items = line.split(CSV_REGEX);
                  return IntStream.range(0, items.length)
                      .mapToObj(i -> Cell.of(header[i], items[i].trim()))
                      .collect(Collectors.toUnmodifiableList());
                }));
  }

  /**
   * transform the input cells to rows as stated by the columns. This method does the following
   * works. 1) filter out the unused cell 2) replace the name by new one 3) convert the string to
   * specified type
   */
  @VisibleForTesting
  Map<Integer, Row> transform(Map<Integer, List<Cell<String>>> indexAndCells) {
    return indexAndCells.entrySet().stream()
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> transform(e.getValue())));
  }

  private Row transform(List<Cell<String>> cells) {
    if (schema.isEmpty()) {
      return Row.of(cells.stream().toArray(Cell[]::new));
    }

    return Row.of(
        schema.stream()
            .sorted(Comparator.comparing(Column::order))
            .map(
                column -> {
                  String value = findCellByName(cells, column.name()).value();
                  Object newValue = convertByType(value, column.dataType());
                  return Cell.of(column.newName(), newValue);
                })
            .toArray(Cell[]::new));
  }

  @VisibleForTesting
  Cell<String> findCellByName(List<Cell<String>> cells, String name) {
    return cells.stream().filter(cell -> cell.name().equals(name)).findFirst().get();
  }

  @VisibleForTesting
  Object convertByType(String value, DataType type) {
    switch (type) {
      case BOOLEAN:
        return Boolean.valueOf(value);
      case BYTE:
        return Byte.valueOf(value);
      case SHORT:
        return Short.valueOf(value);
      case INT:
        return Integer.valueOf(value);
      case LONG:
        return Long.valueOf(value);
      case FLOAT:
        return Float.valueOf(value);
      case DOUBLE:
        return Double.valueOf(value);
      case STRING:
        return value;
      case OBJECT:
        return value;
      default:
        throw new IllegalArgumentException("Unsupported type " + type);
    }
  }

  @VisibleForTesting
  List<RowSourceRecord> toRecords(Map<Integer, Row> rows) {
    return rows.entrySet().stream()
        // we have to keep the order of insertion by sorting key (key is the index of line in file)
        .sorted(Map.Entry.comparingByKey())
        .map(
            e -> {
              int index = e.getKey();
              Row row = e.getValue();
              return toRecords(row, index);
            })
        .flatMap(List::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  @VisibleForTesting
  List<RowSourceRecord> toRecords(Row row, int index) {
    return this.topicKeys.stream()
        .map(
            t ->
                RowSourceRecord.builder()
                    .sourcePartition(partition)
                    .sourceOffset(Map.of(CSV_OFFSET_KEY, index))
                    .row(row)
                    .topicKey(t)
                    .build())
        .collect(Collectors.toUnmodifiableList());
  }

  public static class Builder
      implements oharastream.ohara.common.pattern.Builder<CsvRecordConverter> {
    // Required parameters
    private String path;
    private Set<TopicKey> topicKeys;
    private OffsetCache offsetCache;
    private int maximumNumberOfLines = Integer.MAX_VALUE;

    // Optional parameters - initialized to default values
    private List<Column> schema = List.of();

    private Builder() {}

    public Builder path(String val) {
      path = val;
      return this;
    }

    public Builder topicKeys(Set<TopicKey> topicKeys) {
      this.topicKeys = new HashSet<>(Objects.requireNonNull(topicKeys));
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is Integer.MAX")
    public Builder maximumNumberOfLines(int maximumNumberOfLines) {
      this.maximumNumberOfLines = CommonUtils.requirePositiveInt(maximumNumberOfLines);
      return this;
    }

    public Builder offsetCache(OffsetCache val) {
      offsetCache = val;
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default is empty")
    public Builder schema(List<Column> val) {
      schema = new ArrayList<>(Objects.requireNonNull(val));
      return this;
    }

    @Override
    public CsvRecordConverter build() {
      CommonUtils.requireNonEmpty(path);
      CommonUtils.requireNonEmpty(topicKeys);
      Objects.requireNonNull(offsetCache);
      return new CsvRecordConverter(this);
    }
  }

  private CsvRecordConverter(Builder builder) {
    path = builder.path;
    topicKeys = builder.topicKeys;
    schema = builder.schema;
    cache = builder.offsetCache;
    maximumNumberOfLines = builder.maximumNumberOfLines;
    partition = Map.of(CSV_PARTITION_KEY, builder.path);
  }
}
