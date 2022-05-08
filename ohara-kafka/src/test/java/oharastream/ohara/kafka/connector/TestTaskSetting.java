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

package oharastream.ohara.kafka.connector;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.PropGroup;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.json.StringList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTaskSetting extends OharaTest {

  @Test
  public void nullInput() {
    Assertions.assertThrows(NullPointerException.class, () -> TaskSetting.of(null));
  }

  @Test
  public void emptyInput() {
    TaskSetting.of(Map.of());
  }

  @Test
  public void nullKey() {
    Map<String, String> map = Collections.singletonMap(null, CommonUtils.randomString());
    Assertions.assertThrows(NullPointerException.class, () -> TaskSetting.of(map));
  }

  @Test
  public void emptyKey() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TaskSetting.of(Map.of("", CommonUtils.randomString())));
  }

  @Test
  public void nullValue() {
    Map<String, String> map = Collections.singletonMap(CommonUtils.randomString(), null);
    Assertions.assertThrows(NullPointerException.class, () -> TaskSetting.of(map));
  }

  @Test
  public void emptyValue() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TaskSetting.of(Map.of(CommonUtils.randomString(), "")));
  }

  @Test
  public void testParseBoolean() {
    String key = CommonUtils.randomString();
    boolean value = true;
    TaskSetting config = TaskSetting.of(Map.of(key, String.valueOf(value)));
    Assertions.assertEquals(value, config.booleanValue(key));
    Assertions.assertFalse(config.booleanOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void testParseNonBoolean() {
    String key = CommonUtils.randomString();
    TaskSetting config = TaskSetting.of(Map.of(key, CommonUtils.randomString()));
    Assertions.assertThrows(IllegalArgumentException.class, () -> config.booleanValue(key));
  }

  @Test
  public void testParseShort() {
    String key = CommonUtils.randomString();
    short value = 123;
    TaskSetting config = TaskSetting.of(Map.of(key, String.valueOf(value)));
    Assertions.assertEquals(value, config.shortValue(key));
    Assertions.assertTrue(config.shortOption(key).isPresent());
    Assertions.assertFalse(config.shortOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void testParseInt() {
    String key = CommonUtils.randomString();
    int value = 123;
    TaskSetting config = TaskSetting.of(Map.of(key, String.valueOf(value)));
    Assertions.assertEquals(value, config.intValue(key));
    Assertions.assertTrue(config.intOption(key).isPresent());
    Assertions.assertFalse(config.intOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void testParseLong() {
    String key = CommonUtils.randomString();
    long value = 123;
    TaskSetting config = TaskSetting.of(Map.of(key, String.valueOf(value)));
    Assertions.assertEquals(value, config.longValue(key));
    Assertions.assertTrue(config.longOption(key).isPresent());
    Assertions.assertFalse(config.longOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void testParseDouble() {
    String key = CommonUtils.randomString();
    double value = 123.333;
    TaskSetting config = TaskSetting.of(Map.of(key, String.valueOf(value)));
    Assertions.assertEquals(value, config.doubleValue(key), 0);
    Assertions.assertTrue(config.doubleOption(key).isPresent());
    Assertions.assertFalse(config.doubleOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void testParseStrings() {
    String key = CommonUtils.randomString();
    List<String> ss = Arrays.asList(CommonUtils.randomString(), CommonUtils.randomString());
    TaskSetting config = TaskSetting.of(Map.of(key, StringList.toKafkaString(ss)));
    List<String> ss2 = config.stringList(key);
    Assertions.assertEquals(ss.size(), ss2.size());
    ss.forEach(s -> Assertions.assertEquals(1, ss2.stream().filter(s::equals).count()));
    Assertions.assertFalse(config.stringListOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void testToPropGroup() {
    String key = CommonUtils.randomString();
    PropGroup propGroup =
        PropGroup.of(
            Arrays.asList(
                Map.of("k0", "v0", "k1", "v1", "k2", "v2"), Map.of("k0", "v0", "k1", "v1")));
    TaskSetting config = TaskSetting.of(Map.of(key, propGroup.toJsonString()));
    PropGroup another = config.propGroup(key);
    Assertions.assertEquals(propGroup, another);
    Assertions.assertTrue(config.propGroupOption(key).isPresent());
    Assertions.assertFalse(config.propGroupOption(CommonUtils.randomString()).isPresent());
  }

  @Test
  public void getEmptyColumn() {
    TaskSetting config = TaskSetting.of(Map.of("pgs", "asdasd"));
    Assertions.assertTrue(config.columns().isEmpty());
  }

  @Test
  public void testToDuration() {
    Duration duration = Duration.ofSeconds(10);
    Assertions.assertEquals(duration, CommonUtils.toDuration(duration.toString()));
    Assertions.assertEquals(duration, CommonUtils.toDuration("10 seconds"));
  }
}
