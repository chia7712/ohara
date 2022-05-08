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

import java.util.Arrays;
import java.util.List;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRow extends OharaTest {

  @Test
  public void testEmpty() {
    Assertions.assertEquals(Row.EMPTY, Row.EMPTY);
  }

  @Test
  public void testEquals() {
    Row row = Row.of(Arrays.asList("tag", "tag2"), Cell.of("aa", "aa"), Cell.of("b", 123));
    Row row2 = Row.of(Arrays.asList("tag", "tag2"), Cell.of("aa", "aa"), Cell.of("b", 123));
    Assertions.assertEquals(row, row);
    Assertions.assertEquals(row, row2);
    Assertions.assertEquals(row2, row);
  }

  @Test
  public void testEqualsWithoutTags() {
    Row row = Row.of(Arrays.asList("tag", "tag2"), Cell.of("aa", "aa"), Cell.of("b", 123));
    Row row2 = Row.of(List.of("tag"), Cell.of("aa", "aa"), Cell.of("b", 123));
    Assertions.assertTrue(row.equals(row, false));
    Assertions.assertTrue(row.equals(row2, false));
    Assertions.assertTrue(row2.equals(row, false));
  }

  @Test
  public void testCells() {
    Row row = Row.of(Arrays.asList("tag", "tag2"), Cell.of("aa", "aa"), Cell.of("b", 123));
    Assertions.assertEquals(2, row.size());
    Assertions.assertEquals(2, row.cells().size());
    Assertions.assertEquals(Cell.of("aa", "aa"), row.cell(0));
    Assertions.assertEquals(Cell.of("aa", "aa"), row.cell("aa"));
  }

  @Test
  public void testTags() {
    Row row = Row.of(Arrays.asList("tag", "tag2"), Cell.of("aa", "aa"), Cell.of("b", 123));
    Assertions.assertEquals(2, row.tags().size());
    Assertions.assertEquals("tag", row.tags().get(0));
    Assertions.assertEquals("tag2", row.tags().get(1));
  }

  @Test
  public void duplicateNameIsIllegal() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Row.of(Arrays.asList("tag", "tag2"), Cell.of("aa", "aa"), Cell.of("aa", 123)));
  }

  @Test
  public void composeRow() {
    Row row = Row.of(Cell.of("abc", Row.of(Cell.of("abc", "aaa"))));
    Row row2 = Row.of(Cell.of("abc", Row.of(Cell.of("abc", "aaa"))));
    Assertions.assertEquals(row, row);
    Assertions.assertEquals(row, row2);
    Assertions.assertEquals(row2, row);
  }

  @Test
  public void testOrder() {
    var cell0 = Cell.of("name", "chia");
    var cell1 = Cell.of("ranking", 1);
    var cell2 = Cell.of("single", false);
    var row = Row.of(cell0, cell1, cell2);
    Assertions.assertEquals(cell0, row.cell(0));
    Assertions.assertEquals(cell1, row.cell(1));
    Assertions.assertEquals(cell2, row.cell(2));
  }
}
