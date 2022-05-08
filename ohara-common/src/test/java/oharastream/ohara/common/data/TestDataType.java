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

import java.sql.Time;
import java.util.stream.Collectors;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataType extends OharaTest {

  @Test
  public void testAll() {
    Assertions.assertEquals(DataType.all.size(), DataType.values().length);
  }

  @Test
  public void noDuplicateOrder() {
    Assertions.assertEquals(
        DataType.all.size(),
        DataType.all.stream().map(t -> t.order).collect(Collectors.toUnmodifiableSet()).size());
  }

  @Test
  public void noDuplicateName() {
    Assertions.assertEquals(
        DataType.all.size(),
        DataType.all.stream()
            .map(Enum<DataType>::name)
            .collect(Collectors.toUnmodifiableSet())
            .size());
  }

  @Test
  public void testOrder() {
    DataType.all.forEach(t -> Assertions.assertEquals(t, DataType.of(t.order)));
  }

  @Test
  public void testName() {
    DataType.all.forEach(t -> Assertions.assertEquals(t, DataType.valueOf(t.name())));
  }

  @Test
  public void testOfType() {
    Assertions.assertEquals(DataType.BOOLEAN, DataType.from(false));
    Assertions.assertEquals(DataType.SHORT, DataType.from((short) 1));
    Assertions.assertEquals(DataType.INT, DataType.from(1));
    Assertions.assertEquals(DataType.LONG, DataType.from((long) 1));
    Assertions.assertEquals(DataType.FLOAT, DataType.from((float) 1));
    Assertions.assertEquals(DataType.DOUBLE, DataType.from((double) 1));
    Assertions.assertEquals(DataType.STRING, DataType.from("asd"));
    Assertions.assertEquals(DataType.BYTE, DataType.from((byte) 1));
    Assertions.assertEquals(DataType.BYTES, DataType.from(new byte[2]));
    Assertions.assertEquals(DataType.ROW, DataType.from(Row.of(Cell.of("aa", "aa"))));
    Assertions.assertEquals(DataType.OBJECT, DataType.from(new Time(123123)));
  }
}
