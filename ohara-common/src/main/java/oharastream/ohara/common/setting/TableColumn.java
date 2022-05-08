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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.util.CommonUtils;

/**
 * the description to "table" type. It includes 1) name - column name 2) type - accepted type of
 * value 3) availableItems - the accepted input value
 */
public class TableColumn implements JsonObject, Serializable {
  private static final long serialVersionUID = 1L;

  public enum Type {
    STRING,
    NUMBER,
    BOOLEAN
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<TableColumn> {

    private String name = null;
    private Type type = Type.STRING;
    private Set<String> recommendedValues = Set.of();

    public Builder name(String name) {
      this.name = CommonUtils.requireNonEmpty(name);
      return this;
    }

    @Optional("default is string type")
    public Builder type(Type type) {
      this.type = Objects.requireNonNull(type);
      return this;
    }

    public Builder recommendedValues(Set<String> recommendedValues) {
      this.recommendedValues = Objects.requireNonNull(recommendedValues);
      return this;
    }

    private Builder() {}

    @Override
    public TableColumn build() {
      return new TableColumn(name, type.name().toLowerCase(), recommendedValues);
    }
  }

  private static final String NAME_KEY = "name";
  private static final String TYPE_KEY = "type";
  private static final String RECOMMENDED_VALUES_KEY = SettingDef.RECOMMENDED_VALUES_KEY;

  private final String name;
  private final String type;
  private final Set<String> recommendedValues;

  @JsonCreator
  private TableColumn(
      @JsonProperty(NAME_KEY) String name,
      @JsonProperty(TYPE_KEY) String type,
      @JsonProperty(RECOMMENDED_VALUES_KEY) Set<String> recommendedValues) {
    this.name = CommonUtils.requireNonEmpty(name);
    this.type = CommonUtils.requireNonEmpty(type);
    this.recommendedValues = Objects.requireNonNull(recommendedValues);
  }

  @JsonProperty(NAME_KEY)
  public String name() {
    return name;
  }

  @JsonProperty(TYPE_KEY)
  public String type() {
    return type;
  }

  @JsonProperty(RECOMMENDED_VALUES_KEY)
  public Set<String> recommendedValues() {
    return Collections.unmodifiableSet(recommendedValues);
  }

  // ------------------------[object]------------------------//
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TableColumn) {
      TableColumn another = (TableColumn) obj;
      return name.equals(another.name)
          && type.equals(another.type)
          && recommendedValues.equals(another.recommendedValues);
    }
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
}
