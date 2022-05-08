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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSettingValue extends OharaTest {
  @Test
  public void testEqual() throws IOException {
    SettingValue value =
        SettingValue.of(CommonUtils.randomString(), CommonUtils.randomString(), List.of());
    ObjectMapper mapper = JsonUtils.objectMapper();
    Assertions.assertEquals(
        value,
        mapper.readValue(mapper.writeValueAsString(value), new TypeReference<SettingValue>() {}));
  }

  @Test
  public void testGetter() {
    String name = CommonUtils.randomString(5);
    String value = CommonUtils.randomString(5);
    String error = CommonUtils.randomString(5);
    SettingValue settingValue = SettingValue.of(name, value, List.of(error));
    Assertions.assertEquals(name, settingValue.key());
    Assertions.assertEquals(value, settingValue.value());
    Assertions.assertEquals(1, settingValue.errors().size());
    Assertions.assertEquals(error, settingValue.errors().get(0));
  }

  @Test
  public void nullName() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> SettingValue.of(null, CommonUtils.randomString(), List.of()));
  }

  @Test
  public void emptyName() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SettingValue.of("", CommonUtils.randomString(), List.of()));
  }

  @Test
  public void nullValue() {
    SettingValue value = SettingValue.of(CommonUtils.randomString(), null, List.of());
    Assertions.assertNull(value.value());
  }

  @Test
  public void emptyValue() {
    SettingValue value = SettingValue.of(CommonUtils.randomString(), "", List.of());
    Assertions.assertEquals("", value.value());
  }

  @Test
  public void nullErrors() {
    Assertions.assertThrows(
        NullPointerException.class, () -> SettingValue.of(CommonUtils.randomString(), "", null));
  }

  @Test
  public void emptyError() {
    SettingValue.of(CommonUtils.randomString(), "", List.of());
  }

  @Test
  public void testToString() {
    String name = CommonUtils.randomString(5);
    String value = CommonUtils.randomString(5);
    String error = CommonUtils.randomString(5);
    SettingValue validatedValue = SettingValue.of(name, value, List.of(error));
    Assertions.assertTrue(validatedValue.toString().contains(name));
    Assertions.assertTrue(validatedValue.toString().contains(value));
    Assertions.assertTrue(validatedValue.toString().contains(error));
  }

  @Test
  public void testToValidatedValue() {
    SettingValue value =
        SettingValue.ofJson(
            "{"
                + "\"key\": \"aaaa\","
                + "\"value\": "
                + "\"cccc\","
                + "\"errors\": "
                + "["
                + "\"errrrrrrr\""
                + "]"
                + "}");
    Assertions.assertEquals("aaaa", value.key());
    Assertions.assertEquals("cccc", value.value());
    Assertions.assertEquals(1, value.errors().size());
    Assertions.assertEquals("errrrrrrr", value.errors().get(0));
  }
}
