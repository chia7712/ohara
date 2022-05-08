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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public enum DataType {
  BYTES((short) 0),
  BOOLEAN((short) 1),
  BYTE((short) 2),
  SHORT((short) 3),
  INT((short) 4),
  LONG((short) 5),
  FLOAT((short) 6),
  DOUBLE((short) 7),
  STRING((short) 8),
  OBJECT((short) 9),
  ROW((short) 10),
  CELL((short) 11);

  public final short order;

  /**
   * seek the data type by the index
   *
   * @param order order from data type
   * @return Data type
   */
  public static DataType of(short order) {
    return Stream.of(DataType.values())
        .filter(t -> t.order == order)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("unknown order:" + order));
  }

  /**
   * Check the input type and then return related {@link DataType}
   *
   * @param obj data
   * @return data type
   */
  public static DataType from(Object obj) {
    if (obj instanceof byte[]) return BYTES;
    else if (obj instanceof Byte) return BYTE;
    else if (obj instanceof Boolean) return BOOLEAN;
    else if (obj instanceof Short) return SHORT;
    else if (obj instanceof Integer) return INT;
    else if (obj instanceof Long) return LONG;
    else if (obj instanceof Float) return FLOAT;
    else if (obj instanceof Double) return DOUBLE;
    else if (obj instanceof String) return STRING;
    else if (obj instanceof Cell) return CELL;
    else if (obj instanceof Row) return ROW;
    else if (obj instanceof Serializable) return OBJECT;
    else throw new UnsupportedOperationException(obj.getClass() + " is not supported");
  }

  public static final List<DataType> all = Arrays.asList(DataType.values());

  DataType(short order) {
    this.order = order;
  }
}
