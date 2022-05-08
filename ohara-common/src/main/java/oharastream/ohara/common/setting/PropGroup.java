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

package oharastream.ohara.common.setting;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.util.CommonUtils;

public final class PropGroup implements Iterable<Map<String, String>> {
  public static PropGroup of(List<Map<String, String>> values) {
    return new PropGroup(values);
  }

  public static PropGroup ofJson(String json) {
    return of(JsonUtils.toObject(json, new TypeReference<>() {}));
  }

  public static PropGroup ofColumn(Column column) {
    return ofColumns(List.of(column));
  }

  public static PropGroup ofColumns(List<Column> columns) {
    return of(
        columns.stream().map(PropGroup::toPropGroup).collect(Collectors.toUnmodifiableList()));
  }

  private final List<Map<String, String>> values;

  private PropGroup(List<Map<String, String>> values) {
    this.values =
        values.stream()
            .filter(s -> !s.isEmpty())
            .map(Map::copyOf)
            .collect(Collectors.toUnmodifiableList());
    this.values.forEach(
        props ->
            props.forEach(
                (k, v) -> {
                  CommonUtils.requireNonEmpty(k);
                  CommonUtils.requireNonEmpty(v);
                }));
  }

  public List<Column> toColumns() {
    return values.stream().map(PropGroup::toColumn).collect(Collectors.toUnmodifiableList());
  }

  public Map<String, String> props(int index) {
    return values.get(index);
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }

  public int size() {
    return values.size();
  }

  public int numberOfElements() {
    return values.stream().mapToInt(Map::size).sum();
  }

  /** @return a unmodifiable list of raw data */
  public List<Map<String, String>> raw() {
    // the values is unmodifiable already.
    return values;
  }

  public String toJsonString() {
    return JsonUtils.toString(values);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PropGroup) return ((PropGroup) obj).toJsonString().equals(toJsonString());
    return false;
  }

  @Override
  public int hashCode() {
    return toJsonString().hashCode();
  }

  @Override
  public String toString() {
    return toJsonString();
  }

  private static Column toColumn(Map<String, String> propGroup) {
    return Column.builder()
        .order(Integer.parseInt(propGroup.get(SettingDef.COLUMN_ORDER_KEY)))
        .name(propGroup.get(SettingDef.COLUMN_NAME_KEY))
        .newName(
            propGroup.getOrDefault(
                SettingDef.COLUMN_NEW_NAME_KEY, propGroup.get(SettingDef.COLUMN_NAME_KEY)))
        .dataType(DataType.valueOf(propGroup.get(SettingDef.COLUMN_DATA_TYPE_KEY).toUpperCase()))
        .build();
  }

  public static Map<String, String> toPropGroup(Column column) {
    return Map.of(
        SettingDef.COLUMN_ORDER_KEY, String.valueOf(column.order()),
        SettingDef.COLUMN_NAME_KEY, column.name(),
        SettingDef.COLUMN_NEW_NAME_KEY, column.newName(),
        SettingDef.COLUMN_DATA_TYPE_KEY, column.dataType().name());
  }

  @Override
  public Iterator<Map<String, String>> iterator() {
    return values.iterator();
  }
}
