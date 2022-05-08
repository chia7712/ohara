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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPropGroup extends OharaTest {
  @Test
  public void testToPropGroup() {
    PropGroup propGroup =
        PropGroup.ofJson(
            "[" + "{" + "\"order\": 1," + "\"aa\": \"cc\"," + "\"aa2\": \"cc2\"" + "}" + "]");
    Assertions.assertEquals(1, propGroup.size());
    Assertions.assertEquals("1", propGroup.props(0).get("order"));
    Assertions.assertEquals("cc", propGroup.props(0).get("aa"));
    Assertions.assertEquals("cc2", propGroup.props(0).get("aa2"));
  }

  @Test
  public void testToStringToPropGroup() {
    PropGroup propGroup =
        PropGroup.ofJson(
            "[" + "{" + "\"order\": 1," + "\"aa\": \"cc\"," + "\"aaa\": \"cc\"" + "}" + "]");
    PropGroup another = PropGroup.ofJson(propGroup.toJsonString());
    Assertions.assertEquals(propGroup.size(), another.size());
    Assertions.assertEquals(propGroup.props(0).get("order"), another.props(0).get("order"));
    Assertions.assertEquals(propGroup.props(0).get("aa"), another.props(0).get("aa"));
    Assertions.assertEquals(propGroup.props(0).get("aaa"), another.props(0).get("aaa"));
  }

  @Test
  public void testNullJson() {
    Assertions.assertThrows(NullPointerException.class, () -> PropGroup.ofJson(null));
  }

  @Test
  public void testEmptyJson() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> PropGroup.ofJson(""));
  }

  @Test
  public void testEmpty() {
    Assertions.assertTrue(PropGroup.of(List.of()).isEmpty());
  }

  @Test
  public void testEmpty2() {
    Assertions.assertTrue(PropGroup.of(List.of(Map.of())).isEmpty());
  }

  @Test
  public void testEmpty3() {
    Assertions.assertEquals(0, PropGroup.of(List.of(Map.of())).numberOfElements());
  }

  @Test
  public void testJson() {
    String json = "[" + "{" + "\"order\": 1," + "\"aa\": \"cc\"," + "\"aaa\": \"ccc\"" + "}" + "]";

    PropGroup propGroup = PropGroup.ofJson(json);
    Assertions.assertEquals(1, propGroup.size());
    Assertions.assertEquals("1", propGroup.iterator().next().get("order"));
    Assertions.assertEquals("cc", propGroup.iterator().next().get("aa"));
    Assertions.assertEquals("ccc", propGroup.iterator().next().get("aaa"));
  }

  @Test
  public void testColumns() {
    List<Column> columns =
        Arrays.asList(
            Column.builder()
                .name(CommonUtils.randomString())
                .newName(CommonUtils.randomString())
                .dataType(DataType.STRING)
                .order(5)
                .build(),
            Column.builder()
                .name(CommonUtils.randomString())
                .newName(CommonUtils.randomString())
                .dataType(DataType.STRING)
                .order(1)
                .build());
    PropGroup pgs = PropGroup.ofColumns(columns);
    List<Column> another = pgs.toColumns();
    Assertions.assertEquals(columns.size(), another.size());
    another.forEach(c -> Assertions.assertTrue(columns.contains(c)));
  }

  @Test
  public void parseJson() {
    String json =
        "["
            + "{"
            + "\"order\": 1,"
            + "\"name\": \"cc\","
            + "\"newName\": \"ccc\","
            + "\"dataType\": \"BYTES\""
            + "}"
            + "]";
    PropGroup pgs = PropGroup.ofJson(json);
    List<Column> columns = pgs.toColumns();
    Assertions.assertEquals(1, columns.size());
    Assertions.assertEquals(1, columns.get(0).order());
    Assertions.assertEquals("cc", columns.get(0).name());
    Assertions.assertEquals("ccc", columns.get(0).newName());
    Assertions.assertEquals(DataType.BYTES, columns.get(0).dataType());

    String json2 =
        "[" + "{" + "\"order\": 1," + "\"name\": \"cc\"," + "\"dataType\": \"BYTES\"" + "}" + "]";

    PropGroup pgs2 = PropGroup.ofJson(json2);
    List<Column> columns2 = pgs2.toColumns();
    Assertions.assertEquals(1, columns2.size());
    Assertions.assertEquals(1, columns2.get(0).order());
    Assertions.assertEquals("cc", columns2.get(0).name());
    Assertions.assertEquals("cc", columns2.get(0).newName());
    Assertions.assertEquals(DataType.BYTES, columns2.get(0).dataType());
  }

  @Test
  public void testRemove() {
    PropGroup pgs = PropGroup.of(List.of(Map.of("a", "b")));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> pgs.iterator().remove());
  }

  @Test
  public void testRemoveFromList() {
    PropGroup pgs = PropGroup.of(List.of(Map.of("a", "b")));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> pgs.raw().remove(0));
  }

  @Test
  public void testRemoveFromMap() {
    PropGroup pgs = PropGroup.of(List.of(Map.of("a", "b")));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> pgs.raw().get(0).remove("a"));
  }

  @Test
  public void testConvert() {
    PropGroup propGroup = PropGroup.of(List.of(Map.of("a", "b")));
    PropGroup another = PropGroup.ofJson(propGroup.toJsonString());
    Assertions.assertEquals(propGroup, another);
  }

  @Test
  public void testToColumnWithLowerCase() {
    Column column =
        Column.builder()
            .name(CommonUtils.randomString(10))
            .dataType(DataType.BOOLEAN)
            .order(3)
            .build();
    Map<String, String> raw = new HashMap<>(PropGroup.toPropGroup(column));
    raw.put(
        SettingDef.COLUMN_DATA_TYPE_KEY, raw.get(SettingDef.COLUMN_DATA_TYPE_KEY).toLowerCase());
    PropGroup group = PropGroup.of(List.of(raw));
    Column another = group.toColumns().get(0);
    Assertions.assertEquals(column, another);
  }
}
