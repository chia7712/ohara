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
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCreation extends OharaTest {

  @Test
  public void testEqual() throws IOException {
    Creation creation =
        Creation.of(
            CommonUtils.randomString(5), CommonUtils.randomString(5), CommonUtils.randomString(5));
    ObjectMapper mapper = JsonUtils.objectMapper();
    Assertions.assertEquals(
        creation,
        mapper.readValue(mapper.writeValueAsString(creation), new TypeReference<Creation>() {}));
  }

  @Test
  public void testGetter() {
    String id = CommonUtils.randomString(5);
    String key = CommonUtils.randomString(5);
    String value = CommonUtils.randomString(5);
    Creation creation = Creation.of(id, key, value);
    Assertions.assertEquals(id, creation.name());
    Assertions.assertEquals(value, creation.configs().get(key));
  }

  @Test
  public void nullName() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> Creation.of(null, CommonUtils.randomString(5), CommonUtils.randomString(5)));
  }

  @Test
  public void emptyName() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Creation.of("", CommonUtils.randomString(5), CommonUtils.randomString(5)));
  }

  @Test
  public void nullKey() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> Creation.of(CommonUtils.randomString(5), null, CommonUtils.randomString(5)));
  }

  @Test
  public void emptyKey() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Creation.of(CommonUtils.randomString(5), "", CommonUtils.randomString(5)));
  }

  @Test
  public void nullValue() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> Creation.of(CommonUtils.randomString(5), CommonUtils.randomString(5), null));
  }

  @Test
  public void emptyValue() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> Creation.of(CommonUtils.randomString(5), CommonUtils.randomString(5), ""));
  }

  @Test
  public void testToString() {
    String name = CommonUtils.randomString(5);
    String key = CommonUtils.randomString(5);
    String value = CommonUtils.randomString(5);
    Creation creation = Creation.of(name, key, value);
    Assertions.assertTrue(creation.toString().contains(name));
    Assertions.assertTrue(creation.toString().contains(key));
    Assertions.assertTrue(creation.toString().contains(value));
  }
}
