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

package oharastream.ohara.common.util;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCommonUtils extends OharaTest {

  private final String DRIVE = System.getenv("SystemDrive");

  @Test
  public void testTimer() throws InterruptedException {
    Assertions.assertTrue(CommonUtils.current() != 0);
    CommonUtils.inject(() -> 0);
    Assertions.assertEquals(0, CommonUtils.current());
    TimeUnit.SECONDS.sleep(2);
    Assertions.assertEquals(0, CommonUtils.current());
    CommonUtils.reset();
    Assertions.assertTrue(CommonUtils.current() != 0);
  }

  @Test
  public void testPath() {
    Assertions.assertEquals(String.join(File.separator, "ccc"), CommonUtils.path("ccc"));
    Assertions.assertEquals(
        String.join(File.separator, "ccc", "abc"), CommonUtils.path("ccc", "abc"));
    Assertions.assertEquals(
        String.join(File.separator, "ccc", "abc"), CommonUtils.path("ccc/", "abc"));
    Assertions.assertEquals(
        String.join(File.separator, DRIVE, "ccc", "abc"), CommonUtils.path(DRIVE, "ccc", "abc"));
    Assertions.assertEquals(
        String.join(File.separator, DRIVE, "ccc", "abc"), CommonUtils.path(DRIVE, "ccc/", "abc"));
  }

  @Test
  public void testName() {
    Assertions.assertEquals("ddd", CommonUtils.name("/abc/ddd"));
    Assertions.assertEquals("aaa", CommonUtils.name("/abc/ddd/aaa"));
  }

  @Test
  public void failedToExtractNameFromRootPath() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> CommonUtils.name("/"));
  }

  @Test
  public void testGetAddress() {
    Assertions.assertEquals(CommonUtils.address("localhost"), "127.0.0.1");
    Assertions.assertEquals(CommonUtils.address("127.0.0.1"), "127.0.0.1");
  }

  @Test
  public void listEquals() {
    List<String> list = new MyList<>();
    list.add("a");
    list.add("b");
    List<String> list2 = new MyList<>(list);
    List<String> list3 = new MyList<>(list);
    list3.add("c");

    Assertions.assertNotEquals(list, list2);
    Assertions.assertNotEquals(list, list3);
    Assertions.assertTrue(CommonUtils.equals(list, list2));
    Assertions.assertFalse(CommonUtils.equals(list, list3));
  }

  @Test
  public void mapEquals() {
    Map<String, String> map = new MyMap<>();
    map.put("a", "valueA");
    map.put("b", "valueB");
    Map<String, String> map2 = new MyMap<>(map);
    Map<String, String> map3 = new MyMap<>(map);
    map3.put("c", "valueC");

    Assertions.assertNotEquals(map, map2);
    Assertions.assertNotEquals(map, map3);
    Assertions.assertTrue(CommonUtils.equals(map, map2));
    Assertions.assertFalse(CommonUtils.equals(map, map3));
  }

  @Test
  public void setEquals() {
    Set<String> set = new MySet<>();
    set.add("a");
    set.add("b");
    Set<String> set2 = new MySet<>(set);
    Set<String> set3 = new MySet<>(set);
    set3.add("c");

    Assertions.assertNotEquals(set, set2);
    Assertions.assertNotEquals(set, set3);
    Assertions.assertTrue(CommonUtils.equals(set, set2));
    Assertions.assertFalse(CommonUtils.equals(set, set3));
  }

  @Test
  public void listMap() {
    Map<String, String> map = new MyMap<>();
    map.put("a", "valueA");
    map.put("b", "valueB");
    Map<String, String> map3 = new MyMap<>(map);
    map3.put("c", "valueC");

    List<Map<String, String>> nestedMap = new MyList<>();
    nestedMap.add(new MyMap<>(map));
    nestedMap.add(new MyMap<>(map));

    List<Map<String, String>> nestedMap2 = new MyList<>(); // same
    nestedMap2.add(new MyMap<>(map));
    nestedMap2.add(new MyMap<>(map));

    List<Map<String, String>> nestedMap3 = new MyList<>(); // not same
    nestedMap3.add(new MyMap<>(map));
    nestedMap3.add(new MyMap<>(map3));

    Assertions.assertNotEquals(nestedMap, nestedMap2);
    Assertions.assertNotEquals(nestedMap, nestedMap3);
    Assertions.assertTrue(CommonUtils.equals(nestedMap, nestedMap2));
    Assertions.assertFalse(CommonUtils.equals(nestedMap, nestedMap3));
  }

  @Test
  public void listList() {

    List<String> list = new MyList<>();
    list.add("a");
    list.add("b");
    List<String> list3 = new MyList<>(list);
    list3.add("c");

    List<List<String>> nestedList = new MyList<>();
    nestedList.add(new MyList<>(list));
    nestedList.add(new MyList<>(list));

    List<List<String>> nestedList2 = new MyList<>();
    nestedList2.add(new MyList<>(list));
    nestedList2.add(new MyList<>(list));

    List<List<String>> nestedList3 = new MyList<>();
    nestedList3.add(new MyList<>(list));
    nestedList3.add(new MyList<>(list3));

    Assertions.assertNotEquals(nestedList, nestedList2);
    Assertions.assertNotEquals(nestedList, nestedList3);
    Assertions.assertTrue(CommonUtils.equals(nestedList, nestedList2));
    Assertions.assertFalse(CommonUtils.equals(nestedList, nestedList3));
  }

  @Test
  public void listSet() {
    Set<String> set = new MySet<>();
    set.add("a");
    set.add("b");
    Set<String> set2 = new MySet<>(set);
    Set<String> set3 = new MySet<>(set);
    set3.add("c");

    List<Set<String>> nestedList = new MyList<>();
    nestedList.add(new MySet<>(set));
    nestedList.add(new MySet<>(set2));

    List<Set<String>> nestedList2 = new MyList<>(); // same
    nestedList2.add(new MySet<>(set));
    nestedList2.add(new MySet<>(set2));

    List<Set<String>> nestedList3 = new MyList<>(); // not same
    nestedList3.add(new MySet<>(set));
    nestedList3.add(new MySet<>(set3));

    Assertions.assertNotEquals(nestedList, nestedList2);
    Assertions.assertNotEquals(nestedList, nestedList3);
    Assertions.assertTrue(CommonUtils.equals(nestedList, nestedList2));
    Assertions.assertFalse(CommonUtils.equals(nestedList, nestedList3));
  }

  @Test
  public void mapList() {
    List<String> list = new MyList<>();
    list.add("a");
    list.add("b");
    List<String> list3 = new MyList<>(list);
    list3.add("c");

    Map<String, List<String>> nestedMap = new MyMap<>();
    nestedMap.put("key", new MyList<>(list));
    nestedMap.put("key2", new MyList<>(list));

    Map<String, List<String>> nestedMap2 = new MyMap<>();
    nestedMap2.put("key", new MyList<>(list));
    nestedMap2.put("key2", new MyList<>(list));

    Map<String, List<String>> nestedMap3 = new MyMap<>();
    nestedMap3.put("key", new MyList<>(list));
    nestedMap3.put("key2", new MyList<>(list3));

    Assertions.assertNotEquals(nestedMap, nestedMap2);
    Assertions.assertNotEquals(nestedMap, nestedMap3);
    Assertions.assertTrue(CommonUtils.equals(nestedMap, nestedMap2));
    Assertions.assertFalse(CommonUtils.equals(nestedMap, nestedMap3));
  }

  @Test
  public void mapSet() {
    Set<String> set = new MySet<>();
    set.add("a");
    set.add("b");
    Set<String> set3 = new MySet<>(set);
    set3.add("c");

    Map<String, Set<String>> nestedMap = new MyMap<>();
    nestedMap.put("key", new MySet<>(set));
    nestedMap.put("key2", new MySet<>(set));

    Map<String, Set<String>> nestedMap2 = new MyMap<>();
    nestedMap2.put("key", new MySet<>(set));
    nestedMap2.put("key2", new MySet<>(set));

    Map<String, Set<String>> nestedMap3 = new MyMap<>();
    nestedMap3.put("key", new MySet<>(set));
    nestedMap3.put("key2", new MySet<>(set3));

    Assertions.assertNotEquals(nestedMap, nestedMap2);
    Assertions.assertNotEquals(nestedMap, nestedMap3);
    Assertions.assertTrue(CommonUtils.equals(nestedMap, nestedMap2));
    Assertions.assertFalse(CommonUtils.equals(nestedMap, nestedMap3));
  }

  @Test
  public void mapMap() {
    Map<String, String> map = new MyMap<>();
    map.put("a", "valueA");
    map.put("b", "valueB");
    Map<String, String> map3 = new MyMap<>(map);
    map3.put("c", "valueC");

    Map<String, Map<String, String>> nestedMap = new MyMap<>();
    nestedMap.put("key", new MyMap<>(map));
    nestedMap.put("key2", new MyMap<>(map));

    Map<String, Map<String, String>> nestedMap2 = new MyMap<>();
    nestedMap2.put("key", new MyMap<>(map));
    nestedMap2.put("key2", new MyMap<>(map));

    Map<String, Map<String, String>> nestedMap3 = new MyMap<>();
    nestedMap3.put("key", new MyMap<>(map));
    nestedMap3.put("key2", new MyMap<>(map3));

    Assertions.assertNotEquals(nestedMap, nestedMap2);
    Assertions.assertNotEquals(nestedMap, nestedMap3);
    Assertions.assertTrue(CommonUtils.equals(nestedMap, nestedMap2));
    Assertions.assertFalse(CommonUtils.equals(nestedMap, nestedMap3));
  }

  private static class MyList<T> extends ArrayList<T> {

    private static final long serialVersionUID = 1L;

    MyList() {
      super();
    }

    MyList(List<T> list) {
      super(list);
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  }

  private static class MyMap<K, V> extends HashMap<K, V> {

    private static final long serialVersionUID = 1L;

    MyMap() {
      super();
    }

    MyMap(Map<K, V> m) {
      super(m);
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  }

  private static class MySet<T> extends HashSet<T> {

    private static final long serialVersionUID = 1L;

    MySet() {
      super();
    }

    MySet(Set<T> m) {
      super(m);
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  }

  @Test
  public void testNullString() {
    Assertions.assertThrows(
        NullPointerException.class, () -> CommonUtils.requireNonEmpty((String) null));
  }

  @Test
  public void testEmptyString() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> CommonUtils.requireNonEmpty(""));
  }

  @Test
  public void testNullCollection() {
    Assertions.assertThrows(
        NullPointerException.class, () -> CommonUtils.requireNonEmpty((Collection<?>) null));
  }

  @Test
  public void testEmptyCollection() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireNonEmpty(List.of()));
  }

  @Test
  public void testNullMap() {
    Assertions.assertThrows(
        NullPointerException.class, () -> CommonUtils.requireNonEmpty((Map<?, ?>) null));
  }

  @Test
  public void testEmptyMap() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireNonEmpty(Map.of()));
  }

  @Test
  public void testTemporaryFolder() {
    File f = CommonUtils.createTempFolder(CommonUtils.randomString(10));
    Assertions.assertTrue(f.exists());
    CommonUtils.requireFolder(f);
  }

  @Test
  public void testRequireConnectionPort() {
    // pass since it is ok to bind port on zero
    CommonUtils.requireBindPort(0);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireBindPort(65535 + 10));
    Assertions.assertThrows(IllegalArgumentException.class, () -> CommonUtils.requireBindPort(-1));
  }

  @Test
  public void testRequireBindPort() {
    CommonUtils.requireBindPort(0);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireBindPort(65535 + 10));
    Assertions.assertThrows(IllegalArgumentException.class, () -> CommonUtils.requireBindPort(-1));
  }

  @Test
  public void testRequirePositiveShort() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requirePositiveShort((short) -1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requirePositiveShort((short) 0));
  }

  @Test
  public void testRequirePositiveInt() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requirePositiveInt(-1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requirePositiveInt(0));
  }

  @Test
  public void testRequirePositiveLong() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requirePositiveLong(-1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requirePositiveLong(0));
  }

  @Test
  public void testRequireNonNegativeShort() {
    CommonUtils.requireNonNegativeShort((short) 0);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireNonNegativeShort((short) -1));
  }

  @Test
  public void testRequireNonNegativeInt() {
    CommonUtils.requireNonNegativeInt(0);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireNonNegativeInt(-1));
  }

  @Test
  public void testRequireNonNegativeLong() {
    CommonUtils.requireNonNegativeLong(0);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> CommonUtils.requireNonNegativeLong(-1));
  }

  @Test
  public void testToDuration() {
    Assertions.assertEquals(Duration.ofSeconds(1), CommonUtils.toDuration("1 second"));
    Assertions.assertEquals(Duration.ofSeconds(1), CommonUtils.toDuration("1 seconds"));
    Assertions.assertEquals(Duration.ofSeconds(3), CommonUtils.toDuration("3 seconds"));
    Assertions.assertEquals(Duration.ofDays(1), CommonUtils.toDuration("1 day"));
    Assertions.assertEquals(Duration.ofDays(1), CommonUtils.toDuration("1 days"));
    Assertions.assertEquals(Duration.ofDays(3), CommonUtils.toDuration("3 days"));
  }

  @Test
  public void availablePortShouldBeBiggerThan1024() {
    for (int i = 0; i != 100; ++i) {
      Assertions.assertTrue(CommonUtils.availablePort() > 1024);
    }
  }

  @Test
  public void testParseLines() {
    Map<String, String> args = CommonUtils.parse(Arrays.asList("=aaa", "aaa=", "ccc", "a=b"));

    Assertions.assertEquals(args.size(), 1);
    Assertions.assertEquals(args.get("a"), "b");
  }
}
