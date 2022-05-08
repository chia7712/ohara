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

package oharastream.ohara.common.data;

import java.io.Serializable;
import java.util.Objects;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.util.CommonUtils;

/** implements Serializable ,because akka unmashaller throws java.io.NotSerializableException */
public final class Column implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String name;
  private final String newName;
  private final DataType dataType;
  private final int order;

  private Column(String name, String newName, DataType dataType, int order) {
    this.name = CommonUtils.requireNonEmpty(name);
    this.newName = CommonUtils.requireNonEmpty(newName);
    this.dataType = Objects.requireNonNull(dataType);
    this.order = CommonUtils.requireNonNegativeInt(order);
  }

  public String name() {
    return name;
  }

  public String newName() {
    return newName;
  }

  public DataType dataType() {
    return dataType;
  }

  public int order() {
    return order;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column column = (Column) o;
    return order == column.order
        && Objects.equals(name, column.name)
        && Objects.equals(newName, column.newName)
        && dataType == column.dataType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, newName, dataType, order);
  }

  @Override
  public String toString() {
    return "Column{"
        + "name='"
        + name
        + '\''
        + ", newName='"
        + newName
        + '\''
        + ", dataType="
        + dataType
        + ", order="
        + order
        + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<Column> {
    private String name;
    private String newName;
    private DataType dataType;
    private int order;

    private Builder() {}

    public Builder name(String name) {
      this.name = CommonUtils.requireNonEmpty(name);
      // default the new name is equal to name
      if (newName == null) this.newName = name;
      return this;
    }

    @Optional("default is same to $name")
    public Builder newName(String newName) {
      this.newName = CommonUtils.requireNonEmpty(newName);
      return this;
    }

    public Builder dataType(DataType dataType) {
      this.dataType = Objects.requireNonNull(dataType);
      return this;
    }

    public Builder order(int order) {
      this.order = CommonUtils.requireNonNegativeInt(order);
      return this;
    }

    @Override
    public Column build() {
      return new Column(name, newName, dataType, order);
    }
  }
}
