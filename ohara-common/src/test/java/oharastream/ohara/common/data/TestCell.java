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

import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCell extends OharaTest {

  @Test
  public void testEquals() {
    Cell<String> cell = Cell.of("abc", "abc");
    Assertions.assertEquals(cell, cell);
    Assertions.assertEquals(cell, Cell.of("abc", "abc"));
    Assertions.assertEquals(Cell.of("abc", "abc"), cell);

    Cell<Integer> cell2 = Cell.of("abc", 123);
    Assertions.assertEquals(cell2, cell2);
    Assertions.assertNotEquals(cell, cell2);
    Assertions.assertNotEquals(cell2, cell);

    Cell<byte[]> cell3 = Cell.of("abc", "Adasd".getBytes());
    Assertions.assertEquals(cell3, cell3);
    Assertions.assertEquals(cell3, Cell.of("abc", "Adasd".getBytes()));
    Assertions.assertEquals(Cell.of("abc", "Adasd".getBytes()), cell3);
  }

  @Test
  public void testNullName() {
    Assertions.assertThrows(NullPointerException.class, () -> Cell.of(null, "abc"));
  }

  @Test
  public void testNullValue() {
    Assertions.assertThrows(NullPointerException.class, () -> Cell.of("abc", null));
  }

  @Test
  public void testHashCode() {
    Cell<String> cell = Cell.of("abc", "abc");
    Assertions.assertEquals(cell.hashCode(), cell.hashCode());
    Assertions.assertEquals(cell.hashCode(), Cell.of("abc", "abc").hashCode());

    Cell<byte[]> cell2 = Cell.of("abc", "abc".getBytes());
    Assertions.assertEquals(cell2.hashCode(), cell2.hashCode());
    Assertions.assertEquals(cell2.hashCode(), Cell.of("abc", "abc".getBytes()).hashCode());
  }

  @Test
  public void cellComposeRow() {
    Cell<Row> c = Cell.of("abc", Row.of(Cell.of("abc", "aaa")));
    Assertions.assertEquals(c.name(), "abc");
    Assertions.assertEquals(c.value(), Row.of(Cell.of("abc", "aaa")));
  }
}
