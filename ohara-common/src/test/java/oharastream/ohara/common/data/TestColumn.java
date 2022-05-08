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
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestColumn extends OharaTest {

  @Test
  public void testNullName() {
    Assertions.assertThrows(NullPointerException.class, () -> Column.builder().name(null));
  }

  @Test
  public void testEmptyName() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Column.builder().name(""));
  }

  @Test
  public void testNullNewName() {
    Assertions.assertThrows(NullPointerException.class, () -> Column.builder().newName(null));
  }

  @Test
  public void testEmptyNewName() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Column.builder().newName(""));
  }

  @Test
  public void testNullDataType() {
    Assertions.assertThrows(NullPointerException.class, () -> Column.builder().dataType(null));
  }

  @Test
  public void testEqual() {
    String name = CommonUtils.randomString(10);
    DataType type = DataType.BOOLEAN;
    int order = 100;
    Column column = Column.builder().name(name).dataType(type).order(order).build();
    Assertions.assertEquals(name, column.name());
    Assertions.assertEquals(type, column.dataType());
    Assertions.assertEquals(order, column.order());
  }

  @Test
  public void testEqualWithNewName() {
    String name = CommonUtils.randomString(10);
    String newName = CommonUtils.randomString(10);
    DataType type = DataType.BOOLEAN;
    int order = 100;
    Column column =
        Column.builder().name(name).newName(newName).dataType(type).order(order).build();
    Assertions.assertEquals(name, column.name());
    Assertions.assertEquals(newName, column.newName());
    Assertions.assertEquals(type, column.dataType());
    Assertions.assertEquals(order, column.order());
  }

  @Test
  public void setNameAfterNewName() {
    String name = CommonUtils.randomString(10);
    String newName = CommonUtils.randomString(10);
    DataType type = DataType.BOOLEAN;
    int order = 100;
    Column column =
        Column.builder().newName(newName).name(name).dataType(type).order(order).build();
    Assertions.assertEquals(name, column.name());
    Assertions.assertEquals(newName, column.newName());
    Assertions.assertEquals(type, column.dataType());
    Assertions.assertEquals(order, column.order());
  }
}
