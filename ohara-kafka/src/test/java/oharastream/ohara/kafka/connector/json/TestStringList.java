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

package oharastream.ohara.kafka.connector.json;

import java.util.Arrays;
import java.util.List;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringList extends OharaTest {

  @Test
  public void testToKafkaString() {
    List<String> list = Arrays.asList("a", "bb", "ccc");
    Assertions.assertEquals(list, StringList.ofKafkaList(StringList.toKafkaString(list)));
  }

  @Test
  public void testJsonString() {
    String json = "[\"aaa\", \"cccc\"]";
    List<String> ss = StringList.ofJson(json);
    Assertions.assertEquals(2, ss.size());
    Assertions.assertEquals("aaa", ss.get(0));
    Assertions.assertEquals("cccc", ss.get(1));
  }

  @Test
  public void testInvalidJsonString() {
    String invalid = StringList.toKafkaString(Arrays.asList("a", "ccc"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> StringList.ofJson(invalid));
  }

  @Test
  public void testNullJson() {
    Assertions.assertTrue(StringList.ofJson(null).isEmpty());
  }

  @Test
  public void testEmptyJson() {
    Assertions.assertTrue(StringList.ofJson("").isEmpty());
  }

  @Test
  public void testNullStringJson() {
    Assertions.assertTrue(StringList.ofJson("NULL").isEmpty());
  }

  @Test
  public void testNullStringJson2() {
    Assertions.assertTrue(StringList.ofJson("null").isEmpty());
  }
}
