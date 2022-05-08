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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTopicKey extends OharaTest {

  @Test
  public void testEqual() throws IOException {
    TopicKey key = TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5));
    ObjectMapper mapper = JsonUtils.objectMapper();
    Assertions.assertEquals(
        key, mapper.readValue(mapper.writeValueAsString(key), new TypeReference<KeyImpl>() {}));
  }

  @Test
  public void testGetter() {
    String group = CommonUtils.randomString(5);
    String name = CommonUtils.randomString(5);
    TopicKey key = TopicKey.of(group, name);
    Assertions.assertEquals(group, key.group());
    Assertions.assertEquals(name, key.name());
  }

  @Test
  public void nullGroup() {
    Assertions.assertThrows(
        NullPointerException.class, () -> TopicKey.of(null, CommonUtils.randomString(5)));
  }

  @Test
  public void emptyGroup() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TopicKey.of("", CommonUtils.randomString(5)));
  }

  @Test
  public void nullName() {
    Assertions.assertThrows(
        NullPointerException.class, () -> TopicKey.of(CommonUtils.randomString(5), null));
  }

  @Test
  public void emptyName() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TopicKey.of(CommonUtils.randomString(5), ""));
  }

  @Test
  public void testToString() {
    String group = CommonUtils.randomString(5);
    String name = CommonUtils.randomString(5);
    TopicKey key = TopicKey.of(group, name);
    Assertions.assertTrue(key.toString().contains(group));
    Assertions.assertTrue(key.toString().contains(name));
  }

  @Test
  public void testEqualToOtherKindsOfKey() {
    String group = CommonUtils.randomString();
    String name = CommonUtils.randomString();
    Assertions.assertEquals(ObjectKey.of(group, name), TopicKey.of(group, name));
    Assertions.assertEquals(TopicKey.of(group, name), TopicKey.of(group, name));
    Assertions.assertEquals(ConnectorKey.of(group, name), TopicKey.of(group, name));
  }
}
